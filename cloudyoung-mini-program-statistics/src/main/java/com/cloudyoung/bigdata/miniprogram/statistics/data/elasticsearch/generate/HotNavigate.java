package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate;

import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.common.TargetConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.util.StatisticUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class HotNavigate {

    public static UpdateRequest generateHotNavigateClick(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String hot_type = keys[1];
        String user_id = keys[2];
        String hotNavigate = StatisticUtils.getHotNavigateTarget(hot_type);
        if(hotNavigate != null){
            String docId = appid + ":" + stat_time + ":" + user_id;
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_user, TargetConstant.stat_user_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"user_id\":\"" + user_id + "\",\n" +
                    "\"hot_navigate_info\":{\"" + hotNavigate + "\":{\n" +
                    "\"hot_type\":" + hot_type + ",\n" +
                    "\"click_num\":" + value + "\n" +
                    "}}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr, XContentType.JSON);
            return updateRequest;
        } else {
            return null;
        }
    }

}
