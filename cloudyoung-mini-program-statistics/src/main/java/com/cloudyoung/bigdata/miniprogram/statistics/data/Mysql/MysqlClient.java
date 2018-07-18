package com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql;

import com.cloudyoung.bigdata.common.Constant;
import com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.business.TargetBusiness;
import com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.model.TargetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class MysqlClient {
    private static Logger logger = LoggerFactory.getLogger(MysqlClient.class);

    public static void main(String[] args) throws Exception{
        String cluster = "master:2181";
        int timeout = 6000;
        Constant.init(cluster, timeout);
        Connection connection = getConnection();
        Map<Integer, Map<Integer, TargetInfo>>  targetMap =  TargetBusiness.getTargetMap(connection);
        System.out.println(targetMap);
    }

    /**
     * 获取mysql Connection
     * @return
     */
    public static Connection getConnection() {
        Map<String, Object> mysql_Map = (Map<String, Object>) ((Map) Constant.globalMap.get("mysql")).get("mini-program");
        String url = (String) mysql_Map.get("jdbc.url");
        String driver = (String) mysql_Map.get("jdbc.driver");
        String username = (String) mysql_Map.get("jdbc.username");
        String password = (String) mysql_Map.get("jdbc.password");
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            logger.error("don't find db driver..");
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }




}
