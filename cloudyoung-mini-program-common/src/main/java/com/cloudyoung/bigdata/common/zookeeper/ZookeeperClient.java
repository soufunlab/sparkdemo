package com.cloudyoung.bigdata.common.zookeeper;

import com.cloudyoung.bigdata.common.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClient {

    public static void main(String[] args) throws Exception{
        String cluster = "master:2181";
        String zkPath = "/cloud-young/bigdata/mini-program/conf/mini-program-global";
        String filePath = "E:/yunyang/mini-progrom/cloudyoung-mini-program-calculate/cloudyoung-mini-program-common/src/main/resources/mini-program-global.xml";
        main(cluster, zkPath, filePath);
    }

    public static void main(String cluster, String zkPath, String  filePath) throws Exception {
        int timeout = 6000;
        createPersistentNode(cluster, timeout, zkPath);
        int version = -1;
        byte[] content = FileUtils.readFile(filePath);
        putData(cluster, timeout, zkPath, content, version);
        System.out.println(getNodeData(cluster, timeout, zkPath));
    }

    /**
     * 生成节点
     *
     * @throws Exception
     */
    public static void createPersistentNode(String cluster, int timeout, String path) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(cluster, timeout, null);
        String[] paths = StringUtils.split(path,"/");
        String pathTmp = "";
        for (int index = 0, len = paths.length; index < len; index++) {
            pathTmp += "/";
            pathTmp += paths[index];
            if (!existsNode(zooKeeper, pathTmp)) {
                zooKeeper.create(pathTmp, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        zooKeeper.close();
    }

    /**
     * 获取节点数据
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static String getNodeData(String cluster, int timeout, String path) throws Exception {
        return new String(getNodeDataByte(cluster, timeout, path));
    }

    public static byte[] getNodeDataByte(String cluster, int timeout, String path) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(cluster, timeout, null);
        byte[] datas = zooKeeper.getData(path, null, new Stat());
        zooKeeper.close();
        return datas;
    }


    /**
     * 判断节点是否存在
     *
     * @param zooKeeper
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean existsNode(ZooKeeper zooKeeper, String path) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            return false;
        }
        return true;
    }

    /**
     * 将数据写到节点中
     * @param cluster
     * @param timeout
     * @param path
     * @param data
     * @param version
     * @return
     * @throws Exception
     */
    public static boolean putData(String cluster, int timeout, String path, String data, int version) throws Exception {
        return putData(cluster, timeout, path, data.getBytes(), version);
    }

    public static boolean putData(String cluster, int timeout, String path, byte[] data, int version) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(cluster, timeout, null);
        zooKeeper.setData(path, data, version);
        zooKeeper.close();
        return true;
    }

}
