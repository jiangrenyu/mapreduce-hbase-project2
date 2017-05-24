package com.bonc.dataTransfer;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataTransferMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {

	private String sperator;
	private String family;
	private String[] rowKeyList;
	private String[] qualifierList;
	private String[] hiveFieldList;
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(this.sperator, -1);

		HashMap<String,String> map = new HashMap<String,String>();

		for (int i = 0; i < this.hiveFieldList.length; i++) {
			map.put(this.hiveFieldList[i], data[i]);
		}

		StringBuffer rowKey = new StringBuffer();
		
		for (String s : this.rowKeyList) {
//			System.out.println(map.get(s));			
			rowKey.append((String) map.get(s));
		}

		Put put = new Put(Bytes.toBytes(rowKey.toString()));

		for (String s : this.qualifierList) {
			String qualifier = map.get(s) == null ? "" : (String) map.get(s);
			put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(s), Bytes.toBytes(qualifier));
		}

		context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey.toString())), put);
	
	
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
		String hbaseQualifier = context.getConfiguration().get("conf.Hbase.qualifier");
		String hiveField = context.getConfiguration().get("conf.hive.field");
		String rowKeyField = context.getConfiguration().get("conf.HBase.Rowkeypos");

		if (hiveField.equals("")) {
			throw new RuntimeException("conf.hive.field is null");
		}

		if (hbaseQualifier.equals("")) {
			throw new RuntimeException("conf.Hbase.qualifier is null");
		}

		if (rowKeyField.equals("")) {
			throw new RuntimeException("conf.HBase.Rowkeypos is null");
		}

		this.qualifierList = hbaseQualifier.split(",", -1);
		this.rowKeyList = rowKeyField.split(",", -1);
		this.hiveFieldList = hiveField.split(",", -1);

		this.sperator = context.getConfiguration().get("conf.mapreduce.dataSperator");
		this.family = context.getConfiguration().get("conf.HBase.familyName");
	
	}
	
	
	
}