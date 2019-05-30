package com.atguohua.hd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @Description: HDFS的文件操作系统对象
 * @Date: 2019-05-22 17:33
 */
@Component
public class HDFSFileSystemUtil {
  private static Logger logger = LoggerFactory.getLogger(HDFSFileSystemUtil.class);
  private FileSystem hdfsFileSystem = null; //hdfs 文件系统对象

  @Value("${hdfs.fs.defaultFS}")
  private String fsDefaultFs;
  @Value("${hdfs.HADOOP_USER_NAME}")
  private String hdfs;

  @PostConstruct
  public void initHDFSFileSystem() {
    try {
      //1.获取hdfs配置文件(uri地址、用户名)，用什么权限去访问hdfs集群上的文件
      Configuration conf = new Configuration();

      //2.这里指定使用的是 hdfs文件系统
      conf.set("fs.defaultFS", fsDefaultFs);

      //3.通过这种方式设置java客户端身份
      System.setProperty("HADOOP_USER_NAME", hdfs);
      hdfsFileSystem = FileSystem.get(conf); //这个方式是自动读取hdfs-site.xml

//    URI uri = URI.create("hdfs://10.1.116.20:8020/");   //定义uri
//    FileSystem.get(uri,new Configuration());
    } catch (IOException e) {
      logger.error("hdfs文件系统对象初始化失败!");
      e.printStackTrace();
    }
  }

  /**
   * @return 外部获取hdfs文件系统对象
   */
  public FileSystem getHdfsFileSystem() {
    if (hdfsFileSystem == null) {
      initHDFSFileSystem();
    }
    return hdfsFileSystem;
  }
}
