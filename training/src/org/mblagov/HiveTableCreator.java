package org.mblagov;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveTableCreator {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	 
	  public static void main(String[] args) throws SQLException {
	    try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
	    Statement stmt = con.createStatement();
	    String tableName = "limits_per_hour";
	    String databaseName = "traffic_limits";
//	    stmt.execute("drop database if exists " + databaseName);
//	    stmt.execute("create database " + databaseName);
	    stmt.execute("use " + databaseName);
	    stmt.execute("drop table if exists " + tableName);
	    stmt.execute("create table " + tableName + " (limit_name string, limit_value int) row format delimited fields terminated by \",\"");
	    // show tables
	    String sql = "show tables '" + tableName + "'";
	    System.out.println("Running: " + sql);
	    ResultSet res = stmt.executeQuery(sql);
	    if (res.next()) {
	      System.out.println(res.getString(1));
	    }
	    // describe table
	    sql = "describe " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "\t" + res.getString(2));
	    }
	 
	    sql = "load data local inpath '/home/cloudera/a.txt' overwrite into table " + tableName;
	    stmt.execute(sql);

	    // select * query
	    sql = "select * from " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getInt(2));
	    }
	 
	  }
}
