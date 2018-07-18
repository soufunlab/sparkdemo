package com.cloudyoung.bigdata.miniprogram.statistics.common;

import com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.MysqlClient;
import com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.business.TargetBusiness;
import com.cloudyoung.bigdata.miniprogram.statistics.data.Mysql.model.TargetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class TargetConstant {
    private static Logger logger = LoggerFactory.getLogger(TargetConstant.class);
    public static final String scancar_user_count = "scancar_user_count";


    public static final String append_user_num = "append_user_num";
    public static final String active_num = "active_num";
    public static final String launch_num = "launch_num";


    public static final String wx_authorization_num = "wx_authorization_num";
    public static final String phone_num = "phone_num";
    public static final String male_num = "male_num";
    public static final String female_num = "female_num";
    public static final String gender = "gender";


    public static final String thumb_num  = "thumb_num";
    public static final String comment_num = "comment_num";
    public static final String enshrine_num = "enshrine_num";
    public static final String share_num = "share_num";
    public static final String nolike = "nolike";
    public static final String into_num = "into_num";
    public static final String exit_num = "exit_num";
    public static final String scan_time = "scan_time";



    public static Map<Integer, Map<Integer, TargetInfo>> targetMap = null;

    public static final String append_num = "append_num";
    public static final String calculate_rowkey = "calculate";
    public static final String calculate_day = "day";
    public static final String calculate_hour = "hour";
    public static final String calculate_week = "week";

    /**
     * es索引与类型
     */
    public static String index_user = "stat_user";
    public static String index_page = "stat_page";
    public static String stat_user_day = "stat_user_day";
    public static String stat_page_day = "stat_page_day";

    static {
        init();
    }

    public static void init() {
        try {
            Connection connection = MysqlClient.getConnection();
            targetMap = TargetBusiness.getTargetMap(connection);
        }catch (SQLException e){
            logger.error("init target info error.", e);
        }
    }


}
