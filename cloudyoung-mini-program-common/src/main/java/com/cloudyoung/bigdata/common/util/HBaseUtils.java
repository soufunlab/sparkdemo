package com.cloudyoung.bigdata.common.util;

public class HBaseUtils {

    public static String generateSplitNumber(String baseInfo, int split) {
        int deviceidCode = Math.abs(baseInfo.hashCode());
        int result = deviceidCode % split;
        int len = String.valueOf(split).length();
        return  String.format("%0" +len +"d", result);
    }

}
