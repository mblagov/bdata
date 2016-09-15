package org.mblagov;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.mblagov.beans.HostTraffic;
import org.mblagov.beans.TrafficLimits;
import org.mblagov.enumerations.AlertType;

import scala.Tuple2;
import scala.Tuple3;

import com.google.common.base.Optional;

public class MainApp {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("mblagov-app")
				.setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		HiveContext hiveContext = new HiveContext(sparkContext);
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				sparkContext, Seconds.apply(1));
		streamingContext
				.checkpoint("file:///home/cloudera/workspace/checkpoints");
		List<Row> rowList = hiveContext.table("traffic_limits.limits_per_hour")
				.collectAsList();

		final TrafficLimits trafficLimits = TrafficLimits
				.createWithLimits(rowList);

		final Map<String, Object> kafkaProperties = new HashMap<String, Object>();
		kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"quickstart.cloudera:9092");
		kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		JavaDStream<HostTraffic> stream = streamingContext
				.receiverStream(new TrafficReceiver());
		stream = stream.window(Minutes.apply(5), Seconds.apply(1));

		final String ip = args.length > 0 ? args[0] : "";
		if (ip != null && !ip.isEmpty()) {
			stream = stream.filter(new IpFilter(ip));
		}

		JavaPairDStream<String, Long> pdStream = stream
				.mapToPair(new TrafficToPairMapper());
		JavaPairDStream<String, Tuple3<Long, AlertType, Boolean>> pdStreamWithState = pdStream
				.updateStateByKey(new StateUpdaterFunction2(trafficLimits));
		pdStreamWithState.filter(new FiringEventsFilter()).map(new KeyMapper())
				.foreachRDD(new KafkaNotifier(kafkaProperties));

		streamingContext.start();
		streamingContext.awaitTermination();

	}

	private static final class IpFilter implements
			Function<HostTraffic, Boolean> {
		private final String ip;

		private IpFilter(String ip) {
			this.ip = ip;
		}

		@Override
		public Boolean call(HostTraffic packageTraffic) throws Exception {
			return ip.equals(packageTraffic.getSourceHost())
					|| ip.equals(packageTraffic.getTargetHost());
		}
	}

	private static final class KafkaNotifier implements
			VoidFunction<JavaRDD<Tuple3<Long, AlertType, Boolean>>> {
		private final Map<String, Object> kafkaProperties;

		private KafkaNotifier(Map<String, Object> kafkaProperties) {
			this.kafkaProperties = kafkaProperties;
		}

		@Override
		public void call(JavaRDD<Tuple3<Long, AlertType, Boolean>> finalEvenRDD)
				throws Exception {
			if (!finalEvenRDD.isEmpty()) {
				// we always have single event since handling single ip address
				Tuple3<Long, AlertType, Boolean> eventToProduce = finalEvenRDD
						.first();

				Long trafficAmount = eventToProduce._1();
				AlertType alertType = eventToProduce._2();
				Boolean fireEvent = eventToProduce._3();
				if (fireEvent) {
					KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(
							kafkaProperties);
					List<ProducerRecord<String, String>> producerRecords = createProducerRecords(
							eventToProduce, trafficAmount, alertType, fireEvent);
					for (ProducerRecord<String, String> producerRecord : producerRecords) {
						kafkaProducer.send(producerRecord);
					}
					kafkaProducer.close();
				}
			}
		}

		private List<ProducerRecord<String, String>> createProducerRecords(
				Tuple3<Long, AlertType, Boolean> eventToProduce,
				Long trafficAmount, AlertType alertType, Boolean fireEvent) {

			List<ProducerRecord<String, String>> result = new ArrayList<ProducerRecord<String, String>>();
			StringBuilder messageBuilder = new StringBuilder();
			ProducerRecord<String, String> producerRecord;
			if (AlertType.HIGH_FROM_LOW == alertType
					|| AlertType.LOW_FROM_HIGH == alertType) {
				messageBuilder.append("Alert is gone. Traffic is OK.");
				producerRecord = new ProducerRecord<String, String>(
						"mblagov-app", messageBuilder.toString());
				result.add(producerRecord);
				messageBuilder = new StringBuilder();
			}

			if (AlertType.HIGH == alertType
					|| AlertType.HIGH_FROM_LOW == alertType) {
				messageBuilder.append("New alert! Traffic rate is too high!");
				messageBuilder.append(" Current traffic amount is "
						+ trafficAmount);
			}
			if (AlertType.LOW == alertType
					|| AlertType.LOW_FROM_HIGH == alertType) {
				messageBuilder.append("New alert! Traffic rate is too low!");
				messageBuilder.append(" Current traffic amount is "
						+ trafficAmount);
			}
			if (AlertType.NORMAL_FROM_HIGH == alertType
					|| AlertType.NORMAL_FROM_LOW == alertType) {
				messageBuilder.append("Alert is gone. Traffic is OK.");
			}

			producerRecord = new ProducerRecord<String, String>("mblagov-app",
					messageBuilder.toString());
			result.add(producerRecord);
			return result;
		}
	}

	/*
	 * Discards fake key
	 */
	private static final class KeyMapper
			implements
			Function<Tuple2<String, Tuple3<Long, AlertType, Boolean>>, Tuple3<Long, AlertType, Boolean>> {
		@Override
		public Tuple3<Long, AlertType, Boolean> call(
				Tuple2<String, Tuple3<Long, AlertType, Boolean>> v1)
				throws Exception {
			return v1._2();
		}
	}

	/*
	 * Filters out events, which shouldn't be fired
	 */
	private static final class FiringEventsFilter implements
			Function<Tuple2<String, Tuple3<Long, AlertType, Boolean>>, Boolean> {
		@Override
		public Boolean call(Tuple2<String, Tuple3<Long, AlertType, Boolean>> v1)
				throws Exception {
			return v1._2()._3();
		}
	}

	/*
	 * Updates state of stream by current status (to be used in next iteration)
	 */
	private static final class StateUpdaterFunction2
			implements
			Function2<List<Long>, Optional<Tuple3<Long, AlertType, Boolean>>, Optional<Tuple3<Long, AlertType, Boolean>>> {

		private final TrafficLimits trafficLimits;

		private StateUpdaterFunction2(TrafficLimits trafficLimits) {
			this.trafficLimits = trafficLimits;
		}

		@Override
		public Optional<Tuple3<Long, AlertType, Boolean>> call(
				List<Long> trafficValueList,
				Optional<Tuple3<Long, AlertType, Boolean>> previousStepState)
				throws Exception {
			AlertType previousAlert = previousStepState.isPresent() ? previousStepState
					.get()._2() : AlertType.NOTHING;

			Long sumOfTraffic = 0l;
			for (Long trafficValue : trafficValueList) {
				sumOfTraffic += trafficValue;
			}

			AlertType resultType = previousAlert;
			boolean fireEvent = false;

			if (sumOfTraffic > trafficLimits.getMaxLimit()) {
				if (previousAlert == AlertType.LOW
						|| previousAlert == AlertType.LOW_FROM_HIGH) {
					resultType = AlertType.HIGH_FROM_LOW;
					fireEvent = true;
				} else if (previousAlert != AlertType.HIGH
						&& previousAlert != AlertType.HIGH_FROM_LOW) {
					resultType = AlertType.HIGH;
					fireEvent = true;
				}
			} else if (sumOfTraffic < trafficLimits.getMinLimit()) {
				if (previousAlert == AlertType.HIGH
						|| previousAlert == AlertType.HIGH_FROM_LOW) {
					resultType = AlertType.LOW_FROM_HIGH;
					fireEvent = true;
				} else if (previousAlert != AlertType.LOW
						&& previousAlert != AlertType.LOW_FROM_HIGH) {
					resultType = AlertType.LOW;
					fireEvent = true;
				}
			} else {
				if (previousAlert == AlertType.LOW
						|| previousAlert == AlertType.NOTHING
						|| previousAlert == AlertType.LOW_FROM_HIGH) {
					resultType = AlertType.NORMAL_FROM_LOW;
					fireEvent = true;
				}
				if (previousAlert == AlertType.HIGH
						|| previousAlert == AlertType.HIGH_FROM_LOW) {
					resultType = AlertType.NORMAL_FROM_HIGH;
					fireEvent = true;
				}
			}
			return Optional.of(new Tuple3<Long, AlertType, Boolean>(
					sumOfTraffic, resultType, fireEvent));
		}
	}

	/*
	 * Maps traffic amounts to fakeKey since we need to count for all ips or for
	 * single one. Can be modified to use ip as a key if it is required to
	 * calculate statistics by each ip separately
	 */
	private static final class TrafficToPairMapper implements
			PairFunction<HostTraffic, String, Long> {
		@Override
		public Tuple2<String, Long> call(HostTraffic arg0) throws Exception {
			return new Tuple2<String, Long>("fakeKey", new Long(
					arg0.getTrafficAmount()));
		}
	}
}
