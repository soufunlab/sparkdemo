package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate;

import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.common.TargetConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.util.StatisticUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class Exposure {

    public static UpdateRequest generateExposurePage(String stat_time, String key, long value){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String exposure_type = keys[1];
        String page_type = keys[2];
        String page_id = keys[3];
        String docId = appid + ":" + stat_time + ":" +page_id;
        String exposureTypeTarget = StatisticUtils.getIntoTypeTarget(exposure_type);
        if(exposureTypeTarget != null) {
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_page, TargetConstant.stat_page_day, docId);
            String jsonStr = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"page_type\":\"" + page_type + "\",\n" +
                    "\"page_id\":\"" + page_id + "\",\n" +
                    "\"exposure_info\":{\"" + exposureTypeTarget + "\":{\n" +
                    "\"exposure_type\":" + exposure_type + ",\n" +
                    "\"exposure_num\":" + value + "\n" +
                    "}}\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonStr,XContentType.JSON);
            return updateRequest;
        }
        return null;
    }

}
