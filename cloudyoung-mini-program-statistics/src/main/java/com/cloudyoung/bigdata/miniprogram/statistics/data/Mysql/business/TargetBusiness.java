package com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.business;

import com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.model.TargetInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TargetBusiness {

    /**
     * 获取target map
     * @return
     */
    public static Map<Integer, Map<Integer, TargetInfo>>  getTargetMap(Connection connection) throws SQLException {
        String sql = "select id, category, target_name, data_type, table_name from t_target_info";
        Map<Integer, Map<Integer, TargetInfo>> targetMap = new HashMap<>();
        Statement statement = connection.createStatement();
        ResultSet resulSet = statement.executeQuery(sql);
        while(resulSet.next()){
            int id = resulSet.getInt(1);
            int category = resulSet.getInt(2);
            String target_name = resulSet.getString(3);
            String data_type = resulSet.getString(4);
            String table_name = resulSet.getString(5);
            TargetInfo targetInfo = new TargetInfo();
            targetInfo.setId(id);
            targetInfo.setCategory(category);
            targetInfo.setTableName(table_name);
            targetInfo.setTargetName(target_name);
            targetInfo.setDataType(data_type);
            if(targetMap.containsKey(category)){
                Map<Integer, TargetInfo> targetInfoMap = targetMap.get(category);
                targetInfoMap.put(id, targetInfo);
                targetMap.put(category, targetInfoMap);
            } else {
                Map<Integer, TargetInfo> targetInfoMap = new HashMap<>();
                targetInfoMap.put(id, targetInfo);
                targetMap.put(category, targetInfoMap);
            }
        }
        resulSet.close();
        statement.close();
        return  targetMap;
    }



}
