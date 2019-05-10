package com.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: ${description}
 * @Date: 2019-03-29 14:28
 */
public class HFileDriver {
  private static Logger logger = LoggerFactory.getLogger(HFileDriver.class);

  // com.mr.HFileDriver
  // 输入 /hbase-data/2017 /hbase-data/2017-HFile hbase_dev_pi_b_2018
  // hadoop jar BulkLoad.jar com.mr.HFileDriver /hbase-data/2018 /hbase-data/2018-HFile ghcd:hbase_dev_pi_b_test
  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hbase");
    Configuration hbaseConf = HBaseConfiguration.create();
    //args=new String[]{"f:/2016","http://10.1.116.21:/out","hbase_dev_pi_b_test"}; //本地测试时写入三个参数
    final Path inputFile = new Path(args[0]);
    final Path outputFile = new Path(args[1]);
    HTable hTable = new HTable(hbaseConf, args[2]);
    Job job = Job.getInstance(hbaseConf, "BulkLoad_" + args[2]);
    job.setNumReduceTasks(6);
    job.setJarByClass(HFileDriver.class);
    job.setMapperClass(HFileMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(HFileOutputFormat2.class);
    FileInputFormat.addInputPath(job, inputFile);
    FileOutputFormat.setOutputPath(job, outputFile);
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);

    job.waitForCompletion(true);
    logger.info("Txt文件已转换为HFile文件，开始执行BulkLoad操作");
    LoadIncrementalHFiles load = new LoadIncrementalHFiles(hbaseConf);
    load.doBulkLoad(outputFile, hTable);
    hTable.close();
  }
}