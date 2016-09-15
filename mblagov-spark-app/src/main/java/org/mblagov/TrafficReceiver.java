package org.mblagov;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.packet.format.FormatUtils;
import org.jnetpcap.protocol.network.Ip4;
import org.mblagov.beans.HostTraffic;

/**
 * Receives traffic packets from network and produces events to the stream
 * 
 * @author mblagov
 */
public class TrafficReceiver extends Receiver<HostTraffic> {

	private static final long serialVersionUID = -8070606804043574699L;
	private Pcap pcap;

	public TrafficReceiver() {
		super(StorageLevel.MEMORY_AND_DISK());
	}

	@Override
	public void onStart() {
		StringBuilder errbuf = new StringBuilder(); // For any error msgs
		int snaplen = 64 * 1024; // Capture all packets, no trucation
		int flags = Pcap.MODE_PROMISCUOUS; // capture all packets
		int timeout = 100; // in millis
		pcap = Pcap.openLive("eth1", snaplen, flags, timeout, errbuf);
		if (pcap == null) {
			System.err.printf("Error while opening device for capture: "
					+ errbuf.toString());
			return;
		}

		// handler for packets, which stores packet size in traffic receiver
		final PcapPacketHandler<String> jpacketHandler = new PcapPacketHandler<String>() {

			Ip4 ip4 = new Ip4();

			@Override
			public void nextPacket(PcapPacket packet, String user) {

				if (packet.hasHeader(ip4)) {
					HostTraffic traf = new HostTraffic();
					traf.setSourceHost(FormatUtils.ip(ip4.source()));
					traf.setTargetHost(FormatUtils.ip(ip4.destination()));
					traf.setTrafficAmount(packet.getTotalSize());
					TrafficReceiver.this.store(traf);
				}
			}
		};

		// run in new thread to separate capturing of traffic from main thread
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				pcap.loop(Pcap.LOOP_INFINITE, jpacketHandler, "");
			}
		});
		t.start();

	}

	@Override
	public void onStop() {
		pcap.close();
	}

}
