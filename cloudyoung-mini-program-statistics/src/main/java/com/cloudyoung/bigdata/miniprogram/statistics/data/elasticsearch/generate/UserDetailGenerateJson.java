package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate;

import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import com.cloudyoung.bigdata.miniprogram.statistics.common.TargetConstant;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class UserDetailGenerateJson {


    public static UpdateRequest generateUserEvenJson(String stat_time, String key){
        String[] keys = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C);
        String appid = keys[0];
        String eventId = keys[1];
        String userId = keys[2];
        String docId = appid + ":" + stat_time + ":" +userId;
        String targetName = "";
        boolean mark = false;
        UpdateRequest updateRequest = null;
        if("11".equals(eventId)){
            targetName = "is_new_user";
            mark = true;
        } else if("1".equals(eventId)){
            targetName = "is_new_wx_authorize";
            mark = true;
        }else if("3".equals(eventId)){
            targetName = "is_new_bind_phone";
            mark = true;
        }else if("2".equals(eventId)){
            targetName = "is_new_photo_authorize";
            mark = true;
        }
        if(!"".equals(targetName)) {
            updateRequest = new UpdateRequest(TargetConstant.index_user, TargetConstant.stat_user_day, docId);
            String jsonString ="{\n" +
                    "    \"stat_time\":" + stat_time +",\n" +
                    "    \"appid\":" + appid +",\n" +
                    "    \"user_id\": \"" + userId + "\",\n" +
                    "    \""+ targetName +"\": " + mark + "\n" +
                    "}";
            updateRequest.docAsUpsert(true);
            updateRequest.doc(jsonString, XContentType.JSON);
        }
        return updateRequest;
    }



}
