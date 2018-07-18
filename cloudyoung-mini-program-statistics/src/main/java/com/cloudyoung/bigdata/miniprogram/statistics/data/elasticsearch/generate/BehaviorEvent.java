package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate;

import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.common.TargetConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.util.StatisticUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class BehaviorEvent {


    /**
     * 基于用户统计用户行为事件
     * @param stat_time
     * @param key
     * @param value
     * @return
     */
    public static UpdateRequest generateUserBehaviorEventJson(String stat_time,  String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String page_type = keys[1];
        String event_id = keys[2];
        String user_id = keys[3];
        String docId = appid + ":" + stat_time + ":" + user_id;
        String pageTypeTarget = StatisticUtils.getPageTypeTarget(page_type);
        String enventTarget = StatisticUtils.getEventTypeTarget(event_id);
        if(pageTypeTarget != null && enventTarget != null) {
            pageTypeTarget += "_info";
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_user, TargetConstant.stat_user_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"user_id\":\"" + user_id + "\",\n" +
                    "\"" + pageTypeTarget + "\":{\n" +
                    "\"" + enventTarget + "\":" + value +"\n" +
                    "}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr, XContentType.JSON);
            return updateRequest;
        }
        return null;
    }

    /**
     * 页面事件统计json构造
     * @param stat_time
     * @param key
     * @param value
     * @return
     */
    public static UpdateRequest generatePageEventJson(String stat_time,  String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String page_type = keys[1];
        String page_id = keys[2];
        String event_id = keys[3];
        String docId = appid + ":" + stat_time + ":" + page_id;
        String enventTarget = StatisticUtils.getEventTypeTarget(event_id);
        if(enventTarget != null) {
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_page, TargetConstant.stat_page_day, docId);
            String jsonStr = "{\n" +
                    "\t\"stat_time\":" + stat_time + ",\n" +
                    "\t\"appid\":" + appid + ",\n" +
                    "\t\"page_id\":" + page_id + ",\n" +
                    "\t\"page_type\":"+ page_type +",\n" +
                    "\t\"" + enventTarget + "\": "+ value +"\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr, XContentType.JSON);
            return updateRequest;
        }
        return null;
    }

}
