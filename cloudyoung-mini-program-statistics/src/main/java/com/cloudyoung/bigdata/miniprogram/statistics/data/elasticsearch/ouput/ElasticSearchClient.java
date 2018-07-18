package com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput;


import com.cloudyoung.bigdata.common.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * es 客户端
 */
public class ElasticSearchClient implements Serializable{
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
    private RestHighLevelClient client;
    private BulkRequest bulkRequest;
    private int bulkCount = 1000;
    private boolean autoFlush = false;

    public ElasticSearchClient() {
        Map<String, Object> elasticsearchMap = (Map<String, Object>) ((Map) Constant.globalMap.get("elasticsearch")).get("mini-program");
        String cluster = (String) elasticsearchMap.get("cluster");
        String[] slaves = StringUtils.split(cluster, ",");
        if (slaves.length <= 0) {
            logger.error("elasticsearch configuration is null.");
        }
        HttpHost[] httpHostList = new HttpHost[slaves.length];
        for (int index = 0; index < slaves.length; index++) {
            String[] tmp = StringUtils.split(slaves[index], ":");
            String slave;
            int port = 9200;
            slave = tmp[0];
            if (tmp.length == 2) {
                port = Integer.parseInt(tmp[1]);
            }
            httpHostList[index] = new HttpHost(slave, port);
        }
        this.client = new RestHighLevelClient(
                RestClient.builder(httpHostList)
        );
        this.bulkRequest = new BulkRequest();
    }

    public void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }

    public void setBulkCount(int bulkCount) {
        this.bulkCount = bulkCount;
    }

    public void add(UpdateRequest updateRequest) throws IOException {
        this.bulkRequest.add(updateRequest);
        if (autoFlush && this.bulkRequest.numberOfActions() >= this.bulkCount) {
            flushAndCommit();
        }
    }

    public void add(IndexRequest indexRequest) throws IOException {
        this.bulkRequest.add(indexRequest);
        if (autoFlush && this.bulkRequest.numberOfActions() >= this.bulkCount) {
            flushAndCommit();
        }
    }

    public void add(DeleteRequest deleteRequest) throws IOException {
        this.bulkRequest.add(deleteRequest);
        if (autoFlush && this.bulkRequest.numberOfActions() >= this.bulkCount) {
            flushAndCommit();
        }
    }

    public void add(DocWriteRequest request) throws IOException {
        this.bulkRequest.add(request);
        if (autoFlush && this.bulkRequest.numberOfActions() >= this.bulkCount) {
            flushAndCommit();
        }
    }

    public synchronized void flushAndCommit() throws IOException {
        if(this.bulkRequest.numberOfActions() <= 0){
            return;
        }
        BulkResponse bulkResponse = client.bulk(this.bulkRequest);
        BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            logger.debug("commit data to elasticsearch response {}", itemResponse);
        }
        this.bulkRequest = new BulkRequest();
    }

    public synchronized void close() throws IOException {
        if (client != null) {
            flushAndCommit();
            client.close();
        }
    }

}
