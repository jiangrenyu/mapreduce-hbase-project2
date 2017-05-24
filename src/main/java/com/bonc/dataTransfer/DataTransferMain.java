package com.bonc.dataTransfer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataTransferMain extends Configured implements Tool {
	private Configuration conf;

	public void setConf(Configuration conf) {
		if (this.conf == null)
			this.conf = conf;
	}

	public Configuration getConf() {
		if (this.conf == null) {
			this.conf = HBaseConfiguration.create();
		}
		return this.conf;
	}

	public int run(String[] args) throws Exception {
		Options options = new Options();
		Option option = new Option("i", "inputPath", true, "inputPath");
		options.addOption(option);
		option = new Option("j", "jobName", true, "Mapreduce jobName");
		options.addOption(option);
		option = new Option("c", "confPath", true, "confFilePath");
		options.addOption(option);

		Parser parser = new PosixParser();
		CommandLine commandLine = parser.parse(options, args, true);

		this.conf.addResource(new Path(commandLine.getOptionValue('c')));
		this.conf.set(TableOutputFormat.OUTPUT_TABLE, this.conf.get("conf.HBase.TableName"));

		Job job = Job.getInstance(this.conf, commandLine.getOptionValue('j'));
		job.setJarByClass(DataTransferMain.class);
		job.setMapperClass(DataTransferMapper.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(commandLine.getOptionValue('i')));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new DataTransferMain(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}