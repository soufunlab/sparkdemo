package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput;

import com.cloudyoung.bigdata.common.Constant;
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.UserDetailGenerateJson;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class SavaDataElasticSearch {


    public static void main(String[] args) throws IOException {
        String cluster = "master:2181";
        int timeout = 6000;
        Constant.init(cluster, timeout);
        ElasticSearchClient elasticSearchClient = new ElasticSearchClient();
        elasticSearchClient.setAutoFlush(true);
        int count = 1;
        long startTime = System.currentTimeMillis();
        while (count-- > 0) {
            UpdateRequest updateRequest = UserDetailGenerateJson.generateUserEvenJson("20180524","1:2:433");
            elasticSearchClient.add(updateRequest);
        }
        elasticSearchClient.close();
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);
    }

}

