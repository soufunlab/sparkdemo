package com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format;

import com.cloudyoung.bigdata.common.hbase.model.TableInfo;
import com.cloudyoung.bigdata.common.hbase.HBaseTableInfoConf;
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */

@SuppressWarnings("unused")
public class XCXTableInputFormat extends TableInputFormat implements Configurable {
	protected static Logger logger = LoggerFactory.getLogger(XCXTableInputFormat.class);

	private String tableName;
	private int split;
	private String nameServer = null;
	private List<Pair<byte[], byte[]>> startStopKeyList= new ArrayList<Pair<byte[], byte[]>>();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		HTable table = null;
		boolean closeOnFinish = false;
		try {
			table = (HTable) getTable();
			if (table == null) {
				throw new IOException("No table was provided.");
			}
		} catch (Exception e) {

		}
		if (table == null) {
			initialize(context);
			closeOnFinish = true;
			table = (HTable) getTable();
		}
		try {
			// 获取集群nameServer
			nameServer = context.getConfiguration().get("hbase.nameserver.address", null);
			// 获取Region
			Connection connection = table.getConnection();
			RegionLocator regionLocator = table.getRegionLocator();
			Admin admin = connection.getAdmin();
			RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, admin);
			// 获取表的startkey和endkey
			Pair<byte[][], byte[][]> keys = getStartEndKeys();

			setStartStopList(context);

			// 判定表中只有一个region时,startkey和endkey就是null
			if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {

				if (startStopKeyList.isEmpty()) {
					startStopKeyList.add(new Pair(getScan().getStartRow(), getScan().getStopRow()));
				}
				HRegionLocation regLoc = table.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
				if (null == regLoc) {
					throw new IOException("Expecting at least one region.");
				}
				List<InputSplit> splits = new ArrayList<InputSplit>(1);
				byte[] startRow = (byte[]) startStopKeyList.get(0).getFirst();
				byte[] stopRow = (byte[]) startStopKeyList.get(0).getSecond();
				long regionSize = sizeCalculator.getRegionSize(regLoc.getRegionInfo().getRegionName());

				TableSplit split = new TableSplit(table.getName(), startRow, stopRow, regLoc.getHostnamePort().split(
						Addressing.HOSTNAME_PORT_SEPARATOR)[0], regionSize);
				splits.add(split);
				return splits;
			}

			// 判定表中有多个region时操作
			List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);

			// 遍历表中的region
			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
					continue;
				}
				HRegionLocation hRegionLocation = table.getRegionLocation(keys.getFirst()[i], false);

				// InetSocketAddress创建并解析
				InetSocketAddress isa = new InetSocketAddress(hRegionLocation.getHostname(), hRegionLocation.getPort());
				if (isa.isUnresolved()) {
					logger.warn("Failed resolve " + isa);
				}
				InetAddress regionAddress = isa.getAddress();
				String regionLocation;
				try {
					regionLocation = this.reverseDNS(regionAddress);
				} catch (NamingException e) {
					logger.warn("Cannot resolve the host name for " + regionAddress + " because of " + e);
					regionLocation = hRegionLocation.getHostname();
				}

				if (startStopKeyList.isEmpty()) {
					startStopKeyList = new ArrayList<Pair<byte[], byte[]>>();
					startStopKeyList.add(new Pair(getScan().getStartRow(), getScan().getStopRow()));
				}
				for (int index = 0; index < startStopKeyList.size(); index++) {
					byte[] startRow = (byte[]) startStopKeyList.get(index).getFirst();
					byte[] stopRow = (byte[]) startStopKeyList.get(index).getSecond();
					if (Bytes.compareTo(startRow, stopRow) > 0) {
						continue;
					}

					// 判断给定的start key和stop key是否在该region中
					if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0)
							&& (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
						byte[] splitStart = startRow.length == 0 || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys.getFirst()[i]
								: startRow;
						byte[] splitStop = (Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) && keys.getSecond()[i].length > 0 ? keys
								.getSecond()[i] : stopRow;
						byte[] regionName = hRegionLocation.getRegionInfo().getRegionName();
						long regionSize = sizeCalculator.getRegionSize(regionName);
						TableSplit split = new TableSplit(table.getName(), splitStart, splitStop, regionLocation, regionSize);
						splits.add(split);
						if (logger.isDebugEnabled()) {
							logger.debug("getSplits: split -> " + i + " -> " + split);
						}
					}

					if ((keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0)
							&& (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {

					}
				}
			}

			// 根据"hbase.mapreduce.input.autobalance"配置选项值判定是否做均衡,默认flase
			boolean enableAutoBalance = context.getConfiguration().getBoolean(MAPREDUCE_INPUT_AUTOBALANCE, false);
			if (enableAutoBalance) {
				long totalRegionSize = 0;
				for (int i = 0; i < splits.size(); i++) {
					TableSplit ts = (TableSplit) splits.get(i);
					totalRegionSize += ts.getLength();
				}
				long averageRegionSize = totalRegionSize / splits.size();
				// the averageRegionSize must be positive.
				if (averageRegionSize <= 0) {
					logger.warn("The averageRegionSize is not positive: " + averageRegionSize + ", " + "set it to 1.");
					averageRegionSize = 1;
				}
				return calculateRebalancedSplits(splits, context, averageRegionSize);
			} else {
				return splits;
			}
		} finally {
			if (closeOnFinish) {
				closeTable();
			}
		}
	}

//	public List<Pair<byte[], byte[]>> getStartStopKeyList() {
//		return startStopKeyList;
//	}

//	public void setStartStopKeyList(List<Pair<byte[], byte[]>> startStopList) {
//		if (startStopList.isEmpty()) {
//			return;
//		}
//		startStopKeyList = new ArrayList<Pair<byte[], byte[]>>();
//		startStopKeyList.addAll(startStopList);
//	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setStartStopList(JobContext context) {
		String tableName = context.getConfiguration().get(TableInputFormat.INPUT_TABLE);
		logger.debug("tableName:{}",tableName);
		TableInfo tableInfo = HBaseTableInfoConf.getHBASETABLECONF().get(tableName);
		if (tableInfo == null) {
			logger.error("无法获取Hbase 表信息");
			logger.error("表：{}不存在", tableName);
			return;
		}
		int splitSize = tableInfo.getSplit();
		String time = context.getConfiguration().get(StatisticsConstant.CUSTOM_TIME);
		if (time == null) {
			logger.warn("about rowkey condition ,custion_time not defined!");
		}
		String[] startAndEndTime = StringUtils.split(time, StatisticsConstant.SPLIT_TIME_C);
		logger.debug("startAndEndTime:{} - {}. splits numbers:{}", startAndEndTime[0], startAndEndTime[1], splitSize);
		if (splitSize == 1) {
			startStopKeyList.add(new Pair(Bytes.toBytes(startAndEndTime[0]), Bytes.toBytes(startAndEndTime[1])));
			return;
		}

		int numBit = String.valueOf(splitSize).length();
		for (int index = 0; index < splitSize; index++) {
			String startRowString = String.format("%0" + numBit + "d" + StatisticsConstant.KEY_SEPARATOR + "%s", index, startAndEndTime[0]);
			String stopRowString = String.format("%0" + numBit + "d" + StatisticsConstant.KEY_SEPARATOR + "%s", index, startAndEndTime[1]);
			startStopKeyList.add(new Pair(Bytes.toBytes(startRowString), Bytes.toBytes(stopRowString)));
		}
	}
}
