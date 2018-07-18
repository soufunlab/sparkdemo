package com.cloudyoung.bigdata.miniprogram.statistics.common;

import com.cloudyoung.bigdata.common.hbase.HBaseTableInfoConf;
import com.cloudyoung.bigdata.common.hbase.model.TableInfo;
import com.cloudyoung.bigdata.common.util.XmlUtils;

import java.util.Map;

public class StatisticsConstant {

    public static char splitChar = ':';
    public static String splitStr = ":";

    public static final String KEY_SEPARATOR = ":";
    public static final char KEY_SEPARATOR_C = ':';

    /**
     * 时间分隔符号
     */
    public static final String SPLIT_TIME = ",";
    public static final char SPLIT_TIME_C = ',';

    public static final String CUSTOM_TIME = "hbase.custom.time";

    public static final String NAMESPACE = "lovecar_dev";
    public static final String family = "info";



    public static final String HOUR_PATTERN = "yyyyMMddHH";
    public static final String DAY_PATTERN = "yyyyMMdd";


    public static final String calculate_rowkey = "calculate";
    public static final String calculate_day = "day";
    public static final String calculate_hour = "hour";
    public static final String calculate_week = "week";



}
