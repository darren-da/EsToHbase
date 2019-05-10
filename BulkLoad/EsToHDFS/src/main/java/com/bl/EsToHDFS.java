package com.bl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Description: ${description}
 * @Date: 2019-03-27 10:46
 */
@SpringBootApplication
public class EsToHDFS {
  public static void main(String[] args) {
    System.setProperty("HADOOP_USER_NAME", "hbase");
    SpringApplication.run(EsToHDFS.class, args);
  }
}

// 1.通过接口将数据写入本地，以day为一个文件进行存储 package:source
// 2.将本地的文件上传到 hdfs 上
// 3.使用 mapReduce 将 hdfs 上的文件变成以 HFile 格式的文件
// 4.将 HFile 格式的文件存入 HBase的Region中
