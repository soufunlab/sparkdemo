package com.cloudyoung.bigdata.miniprogram.statistics.main;

import com.cloudyoung.bigdata.common.util.FileUtils;
import com.cloudyoung.bigdata.common.zookeeper.ZookeeperClient;

public class ZookeeperClientStatic {

    public static void main(String[] args) throws  Exception{
        String cluster = "master:2181";
        int timeout = 6000;
        String path = "/cloud-young/bigdata/mini-program/conf/mini-program-global";
        ZookeeperClient.createPersistentNode(cluster, timeout, path);
        String data = "ttt";
        int version = -1;
        byte[] content = FileUtils.readFile("E:\\git\\cloudyoung-mini-program-calculate\\cloudyoung-mini-program-common\\src\\main\\resources\\mini-program-global.xml");
        ZookeeperClient.putData(cluster, timeout, path, content, version);
        System.out.println(ZookeeperClient.getNodeData(cluster, timeout, path));

        String path1 = "/cloud-young/bigdata/mini-program/conf/hbase-table-info";
        ZookeeperClient.createPersistentNode(cluster,timeout,path1);
        String data1 = "sss";
        int version1 = -1;
        byte[] content1 = FileUtils.readFile("E:\\git\\cloudyoung-mini-program-calculate\\cloudyoung-mini-program-common\\src\\main\\resources\\hbase-table-info.xml");
        ZookeeperClient.putData(cluster,timeout,path1,content1,version1);
        System.out.println(ZookeeperClient.getNodeData(cluster,timeout,path1));







    }



}
