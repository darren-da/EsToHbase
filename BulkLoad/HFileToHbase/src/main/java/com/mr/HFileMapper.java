package com.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读入文件的数据结构 <inKey,inValue,outKey,outValue>
 * <当前index,取出的value,输出的rowkey,Put对象>
 */
public class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
  private static final byte[] COLUNMFAMILY = Bytes.toBytes("pi");
  private static final byte[] PI_ID = Bytes.toBytes("pi_id");
  private static final byte[] ORIGINAL_NAME = Bytes.toBytes("original_name");
  private static final byte[] PI_CODE = Bytes.toBytes("pi_code");
  private static final byte[] REGULATORY_AGENCY = Bytes.toBytes("regulatory_agency");
  private static final byte[] TIME = Bytes.toBytes("time");
  private static final byte[] POINT_VALUE = Bytes.toBytes("point_value");
  private static final byte[] POINT_QUALITY = Bytes.toBytes("point_quality1");


  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] items = value.toString().split(",,");
    for (String item : items) {
      String[] itemArray = item.split(",");
      if (itemArray.length!=6){
        System.out.println(itemArray);//解决数组下标越界问题
      }else {
          ImmutableBytesWritable putRowkey = new ImmutableBytesWritable(Bytes.toBytes(itemArray[0] + itemArray[3] + itemArray[1]));
          Put put = new Put(Bytes.toBytes(itemArray[0] + itemArray[3] + itemArray[1])); //以 pi_id + "_" + stampTime = rowkey
          put.addColumn(COLUNMFAMILY, ORIGINAL_NAME, Bytes.toBytes(itemArray[1])); // original_name
          put.addColumn(COLUNMFAMILY, PI_CODE, Bytes.toBytes(itemArray[2])); //pi_code
          put.addColumn(COLUNMFAMILY, REGULATORY_AGENCY, Bytes.toBytes(itemArray[3])); // regulatory_agency
          put.addColumn(COLUNMFAMILY, TIME, Bytes.toBytes(itemArray[0])); //time
          put.addColumn(COLUNMFAMILY, POINT_VALUE, Bytes.toBytes(itemArray[4])); // point_value
          put.addColumn(COLUNMFAMILY, POINT_QUALITY, Bytes.toBytes(itemArray[5])); //point_quality1
//      logger.info(" ========== " + putRowkey + " ==========");
          context.write(putRowkey, put);
      }
    }
  }
}
