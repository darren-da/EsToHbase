package com.atguohua.hd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Hdfs2hbaseApplication {
    private static Logger logger = LoggerFactory.getLogger(Hdfs2hbaseApplication.class);
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hbase");
        SpringApplication.run(Hdfs2hbaseApplication.class, args);
        logger.info("工程启动成功！");
        System.err.print("程序顺利运行完成！");

    }

}
