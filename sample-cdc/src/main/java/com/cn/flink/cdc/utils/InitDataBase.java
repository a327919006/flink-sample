package com.cn.flink.cdc.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class InitDataBase {
    private static final Logger logger = LoggerFactory.getLogger(InitDataBase.class);

    public static Connection initMysql(ParameterTool parameterTool) {
        try {
            String mysqlUrl = parameterTool.get("mysql.url", "jdbc:mysql://192.168.5.132:6033/cnte_iot_device?useSSL=false&autoReconnect=true");
            System.out.println("mysqlUrl" + mysqlUrl);
            String mysqlUsername = parameterTool.get("mysql.username", "cnte");
            System.out.println("mysqlusername" + mysqlUsername);
            String mysqlPassword = parameterTool.get("mysql.password", "Cnte@19808");
            System.out.println("mysqlpassword" + mysqlPassword);
            return DriverManager.getConnection(mysqlUrl, mysqlUsername, mysqlPassword);
        } catch (SQLException sqlException) {
            logger.error("mysql连接失败");
            logger.error(sqlException.getLocalizedMessage());
            logger.error(sqlException.getMessage());
            logger.error(sqlException.getSQLState());
            logger.error(sqlException.getCause().toString());
            return null;
        }
    }
}
