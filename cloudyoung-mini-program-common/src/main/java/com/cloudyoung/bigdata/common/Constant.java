package com.cloudyoung.bigdata.common;

import com.cloudyoung.bigdata.common.hbase.HBaseTableInfoConf;
import com.cloudyoung.bigdata.common.hbase.model.TableInfo;
import com.cloudyoung.bigdata.common.util.XmlUtils;
import com.cloudyoung.bigdata.common.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

public class Constant {
    private static Logger logger = LoggerFactory.getLogger(Constant.class);
    public static char splitChar = ':';
    public static String splitStr = ":";

    public static final String KEY_SEPARATOR = ":";
    public static final char KEY_SEPARATOR_C = ':';

    /**
     * 时间分隔符号
     */
    public static final String SPLIT_TIME = ",";
    public static final char SPLIT_TIME_C = ',';

    public static final String CUSTOM_TIME = "hbase.custom.time";

    public static Map<String, Object> globalMap;

    static {
        String cluster = "devbmaster:2181";
        int timeout = 6000;
        init(cluster, timeout);
    }

    public static void init(String cluster, int timeout) {
        try {
//            String globalPath = Constant.class.getClassLoader().getResource("mini-program-global.xml").getPath();
//            String tableInfo = Constant.class.getClassLoader().getResource("hbase-table-info.xml").getPath();
            String tableInfoPath = "/cloud-young/bigdata/mini-program/conf/hbase-table-info";
            String globalPath = "/cloud-young/bigdata/mini-program/conf/mini-program-global";
            byte[] tableInfoBytes = ZookeeperClient.getNodeDataByte(cluster, timeout, tableInfoPath);
            byte[] globalBytes = ZookeeperClient.getNodeDataByte(cluster, timeout, globalPath);
            InputStream tableInfoInputStream = new ByteArrayInputStream(tableInfoBytes);
            InputStream globalInputStream = new ByteArrayInputStream(globalBytes);
            globalMap = XmlUtils.parseGlobalXML(globalInputStream);
            Map<String, TableInfo> tableInfoMap = XmlUtils.parseOptions(tableInfoInputStream);
            HBaseTableInfoConf.HBASETABLECONF = tableInfoMap;
            logger.info("---------------------------------------------------success");
        } catch (Exception e) {
            logger.error("configuration init error", e);
            System.exit(0);
        }
    }
}
