package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate;

import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.common.TargetConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.util.StatisticUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class BrowsePage {

    /**
     * 基于用户页面浏览次数
     * @param stat_time
     * @param key
     * @param value
     * @return
     */
    public static UpdateRequest generateUserBrowsePageNum(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String user_id = keys[1];
        String page_type = keys[2];
        String pageTypeTarget = StatisticUtils.getPageTypeTarget(page_type);
        if(pageTypeTarget != null){
            String docId = appid + ":" + stat_time + ":" +user_id;
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_user, TargetConstant.stat_user_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"user_id\":\"" + user_id + "\",\n" +
                    "\"" + pageTypeTarget + "\":{\n" +
                    "\"scan_num\":" + value +"\n" +
                    "}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr, XContentType.JSON);
            return updateRequest;
        }
        return null;
    }


    /**
     * 基于用户页面浏览时长
     * @param stat_time
     * @param key
     * @param value
     * @return
     */
    public static UpdateRequest generateUserBrowsePageTime(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String user_id = keys[1];
        String page_type = keys[2];
        String pageTypeTarget = StatisticUtils.getPageTypeTarget(page_type);
        if(pageTypeTarget != null){
            String docId = appid + ":" + stat_time + ":" +user_id;
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_user, TargetConstant.stat_user_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"user_id\":\"" + user_id + "\",\n" +
                    "\"" + pageTypeTarget + "\":{\n" +
                    "\"scan_time\":" + value +"\n" +
                    "}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr, XContentType.JSON);
            return updateRequest;
        }
        return null;
    }

    public static UpdateRequest generatePageScanNum(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String into_type = keys[1];
        String page_type = keys[2];
        String page_id = keys[3];
        String docId = appid + ":" + stat_time + ":" +page_id;
        String intoTypeTarget = StatisticUtils.getIntoTypeTarget(into_type);
        if(intoTypeTarget != null) {
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_page, TargetConstant.stat_page_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"page_type\":\"" + page_type + "\",\n" +
                    "\"page_id\":\"" + page_id + "\",\n" +
                    "\"page_scan_info\":{\"" + intoTypeTarget + "\":{\n" +
                    "\"into_type\":" + into_type + ",\n" +
                    "\"scan_num\":" + value + "\n" +
                    "}}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr,XContentType.JSON);
            return updateRequest;
        }
        return null;
    }

    public static UpdateRequest generatePageScanPeopleNum(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String into_type = keys[1];
        String page_type = keys[2];
        String page_id = keys[3];
        String docId = appid + ":" + stat_time + ":" +page_id;
        String intoTypeTarget = StatisticUtils.getIntoTypeTarget(into_type);
        if(intoTypeTarget != null) {
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_page, TargetConstant.stat_page_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"page_type\":\"" + page_type + "\",\n" +
                    "\"page_id\":\"" + page_id + "\",\n" +
                    "\"page_scan_info\":{\"" + intoTypeTarget + "\":{\n" +
                    "\"into_type\":" + into_type + ",\n" +
                    "\"scan_p_num\":" + value + "\n" +
                    "}}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr,XContentType.JSON);
            return updateRequest;
        }
        return null;
    }

    public static UpdateRequest generatePageScanTime(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String into_type = keys[1];
        String page_type = keys[2];
        String page_id = keys[3];
        String docId = appid + ":" + stat_time + ":" +page_id;
        String intoTypeTarget = StatisticUtils.getIntoTypeTarget(into_type);
        if(intoTypeTarget != null) {
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_page, TargetConstant.stat_page_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"page_type\":\"" + page_type + "\",\n" +
                    "\"page_id\":\"" + page_id + "\",\n" +
                    "\"page_scan_info\":{\"" + intoTypeTarget + "\":{\n" +
                    "\"into_type\":" + into_type + ",\n" +
                    "\"scan_time\":" + value + "\n" +
                    "}}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr,XContentType.JSON);
            return updateRequest;
        }
        return null;
    }
}
