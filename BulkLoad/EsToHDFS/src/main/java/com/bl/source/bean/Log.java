package com.bl.source.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Log {
  private String plant;    //电厂编号
  private String project_start_time;  //任务开始时间
  private String project_end_time;  //任务结束时间
  private String data_start_time;  //数据开始时间
  private String data_end_time;  //数据结束时间
  private String all_time;  //总耗时
  private String select_time;  //查询耗时
  private String invert_time;  //数据插入耗时
  private String put_hbase_time;  //放入hbase耗时
  private String success;  //成功数
  private String fail;  //失败数
  private String no_value_count;  //无值的tag数
  private String no_create_count;  //没创建的tag数
  private String fail_tag_count;  //失败的tag数
  private String tags_size;  //本次程序的测点总数
}
