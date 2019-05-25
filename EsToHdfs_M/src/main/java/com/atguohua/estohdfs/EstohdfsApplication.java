package com.atguohua.estohdfs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class EstohdfsApplication {


    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hbase");
        SpringApplication.run(EstohdfsApplication.class, args);
        System.out.println("程序启动成功！");


    }

}
// 1.通过接口将数据写入本地，以day为一个文件进行存储 package:source
// 2.将本地的文件上传到 hdfs 上
