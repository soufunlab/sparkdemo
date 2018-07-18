package com.cloudyoung.bigdata.common.hbase;

import com.cloudyoung.bigdata.common.hbase.model.TableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseClient {
    private static Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    public static boolean namespaceExists(String namespaceName) {
        Connection connection = null;
        Admin admin = null;
        try {
            Configuration conf = HBaseContext.getHbaseConf();
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(namespaceName);
            logger.info("获取表空间信息：" + namespaceDescriptor.getConfiguration());
            return Boolean.TRUE;
        } catch (IOException e) {
            logger.error("获取命名空间失败，查看日志查看命名空间" + namespaceName + "是否存在", e);
            return Boolean.FALSE;
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                }
            }
        }
    }


    public static boolean createNamespace(String namespaceName) {
        Connection connection = null;
        Admin admin = null;
        try {
            Configuration conf = HBaseContext.getHbaseConf();
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespaceName).build();
            admin.createNamespace(namespaceDescriptor);
            return Boolean.TRUE;
        } catch (IOException e) {
            return Boolean.FALSE;
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                }
            }
        }
    }


    public static boolean createTable(TableInfo tableInfo) {
        Connection connection = null;
        Admin admin = null;
        String namespace = tableInfo.getNameSpace();
        String tablename = tableInfo.getName();
        int ttl = tableInfo.getTtl();
        int split = tableInfo.getSplit();
        byte[][] splitKeys = calcSplitKeys(split);
        try {
            Configuration conf = HBaseContext.getHbaseConf();
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            if (null != namespace && !"".equals(namespace)) {
                tablename = namespace + ":" + tablename;
            }
            TableName tName = TableName.valueOf(tablename);
            if (admin.tableExists(tName)) {
                logger.info(tablename + "表已经存在！");
                return Boolean.FALSE;
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tName);
                tableDesc.addFamily(new HColumnDescriptor(tableInfo.getFamily()).setTimeToLive(ttl));
                admin.createTable(tableDesc, splitKeys);
                logger.info(tablename + ":表创建成功！");
                return Boolean.TRUE;
            }
        } catch (IOException e) {
            logger.error(tablename + ":创建表失败", e);
            return Boolean.FALSE;
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                }
            }
        }
    }


    public static byte[][] calcSplitKeys(int partition) {
        byte[][] splitKeys = new byte[partition][];
        int len = String.valueOf(partition).length();
        for (int i = 0; i < partition; i++) {
            splitKeys[i] = Bytes.toBytes(String.format("%0" + len + "d", i));
        }
        return splitKeys;
    }


}
