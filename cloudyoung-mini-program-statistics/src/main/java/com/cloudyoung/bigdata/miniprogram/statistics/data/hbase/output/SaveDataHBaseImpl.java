package com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.output;

import com.cloudyoung.bigdata.common.hbase.HBaseContext;
import com.cloudyoung.bigdata.common.util.HBaseUtils;
import com.cloudyoung.bigdata.common.hbase.HBaseTableInfoConf;
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;


/**
 * 将数据存储到hbase
 */
public class SaveDataHBaseImpl implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(SaveDataHBaseImpl.class);

    private static Configuration conf = null;
    private static HConnection connection = null;
    private HTableInterface table = null;
    private byte[] columnFamily;
    private String tablename;
    private int split;

    static {
        try {
            conf = HBaseContext.getHbaseConf();
            connection = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SaveDataHBaseImpl(String tablename, String columnFamily) {
        try {
            this.columnFamily = Bytes.toBytes(columnFamily);
            if (!tablename.startsWith(StatisticsConstant.NAMESPACE)) {
                this.tablename = StatisticsConstant.NAMESPACE + StatisticsConstant.KEY_SEPARATOR + tablename;
            } else
                this.tablename = tablename;
            this.table = connection.getTable(this.tablename);
            this.split = HBaseTableInfoConf.getHBASETABLECONF().get(this.tablename).getSplit();
            table.setAutoFlush(false);
            table.setWriteBufferSize(10 * 1024 * 1024);// 10M
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void saveRowData(String rowKey, String colName, String colValue) {
        rowKey = HBaseUtils.generateSplitNumber(rowKey,this.split ) + StatisticsConstant.KEY_SEPARATOR_C + rowKey;
        Put p = new Put(Bytes.toBytes(rowKey));
        p.setDurability(Durability.SKIP_WAL);
        if (colName != null && colValue != null) {
            p.add(columnFamily, Bytes.toBytes(colName), Bytes.toBytes(colValue));
        } else {
            p.add(columnFamily, Bytes.toBytes(""), Bytes.toBytes(""));
        }
        try {
            this.table.put(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void closeTable() {
        try {
            if (this.table != null) {
                this.table.flushCommits();
                this.table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("closeTable error!", e);
        }
    }


}
