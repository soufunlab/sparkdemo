package com.cloudyoung.bigdata.common.hbase;

import com.cloudyoung.bigdata.common.hbase.model.TableInfo;
import org.apache.hadoop.fs.FileSystem;

import java.util.Map;

public class HBaseTableInfoConf {

	/**
	 * HBASE表配置信息
	 */
	public static Map<String, TableInfo> HBASETABLECONF;

	private static FileSystem fs = null;

	// static{
	//
	// HBASETABLECONF=XMLUtil.getTableInfo("/hbasetableInfo.xml");
	// }
	public static Map<String, TableInfo> getHBASETABLECONF() {
		return HBASETABLECONF;
	}

}
