package com.atguohua.hd.hdfs.mv.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: hdfs文件的实体Bean
 * @Date: 2019-05-22 19:02
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileBean {
  private String fileName; //文件名
  private String filePath; //文件路径

}
