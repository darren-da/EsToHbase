package com.bl.source.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Item {
  private String plant;   //电厂编号
  private String data_start_time; //数据开始时间
  private String data_end_time;  //数据结束时间
  private String tag;  // tag
  private String tag_count;  // tag数量
  private String project_start_time;  // 任务开始时间
  private String project_end_time;  // 任务结束时间
  private String plants_size;  // 此次任务所有的测点个数,区分引风机与全厂


  public Item(String plant, String data_start_time, String data_end_time, String tag, String tag_count,String plants_size) {
    this.plant = plant;
    this.data_start_time = data_start_time;
    this.data_end_time = data_end_time;
    this.tag = tag;
    this.tag_count = tag_count;
    this.plants_size = plants_size;
  }
}
