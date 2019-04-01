package com.virtusai.clickhouseclient.utils;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ChClietn {

 public ClickHouseConnection getConn (String user,String password,String db,String url ){
     ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
     ClickHouseConnection conn=null;
     clickHouseProperties.setUser(user);
     clickHouseProperties.setPassword(password);
     clickHouseProperties.setDatabase(db);
     ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(url, clickHouseProperties);
     try {
         conn=clickHouseDataSource.getConnection();
     } catch (SQLException e) {
         e.printStackTrace();
     }
     return conn;


 }
 public  void  release(Connection connection, PreparedStatement pstmt){

         try {
             if (pstmt != null) {
             pstmt.close();
         }
         } catch (SQLException e) {
             e.printStackTrace();
         } finally {
             if (connection != null) {
                 try {
                     connection.close();
                 } catch (SQLException e) {
                     e.printStackTrace();
                 }
             }
         }


 }
}
