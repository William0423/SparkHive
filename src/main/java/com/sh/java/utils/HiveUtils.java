package com.sh.java.utils;

import java.sql.*;

public class HiveUtils {

    // 备注，不是private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        // 备注：不是jdbc:hive://localhost:10000/default", "", ""
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hadoop", "hive");// 此处的hadoop是服务器对应的user的name名称；hive是表名
        Statement stmt = con.createStatement();

        String tableName = "hive_bdtable";
        // select * query
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + " " + res.getDouble(2) + " " + res.getDouble(3));
        }


        System.out.println("Database userdb created successfully.");

        con.close();
    }
}
