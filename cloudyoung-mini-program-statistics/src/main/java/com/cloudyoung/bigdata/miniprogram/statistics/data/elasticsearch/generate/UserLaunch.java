package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate;

import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.common.TargetConstant;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class UserLaunch {

    /**
     * 基于用户页面浏览次数
     *
     * @param stat_time
     * @param key
     * @param value
     * @return
     */
    public static UpdateRequest generateUserLaunchNum(String stat_time, String key, long value) {
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[2];
        String user_id = keys[3];
        String targetName = "launch_num";
        if ("launch_num".equals(targetName)) {
            String docId = appid + ":" + stat_time + ":" + user_id;
            UpdateRequest updateRequest = new UpdateRequest(TargetConstant.index_user, TargetConstant.stat_user_day, docId);
            String jsonString = "{\n" +
                    "\"stat_time\":" + stat_time + ",\n" +
                    "\"appid\":" + appid + ",\n" +
                    "\"user_id\":\"" + user_id + "\",\n" +
                    "\"launch_num\":" + value + "\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonString, XContentType.JSON);
            return updateRequest;
        }
        return null;


    }


}
