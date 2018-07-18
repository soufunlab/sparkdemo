package com.cloudyoung.bigdata.common.hbase;

import com.cloudyoung.bigdata.common.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;

import java.io.IOException;
import java.util.Map;

public class HBaseContext {
    public static Configuration getHbaseConf() {
        Configuration conf = new Configuration();
        Map<String, Object> hbaseConfMap = (Map)Constant.globalMap.get("hbase");
        for(Map.Entry<String, Object> entry : hbaseConfMap.entrySet()){
            String key = entry.getKey();
            String value = (String)entry.getValue();
            conf.set(key, value);
        }
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return conf;
    }

    public static String convertScanToString(Scan scan) {
        ClientProtos.Scan proto = null;
        try {
            proto = ProtobufUtil.toScan(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Base64.encodeBytes(proto.toByteArray());
    }

}
