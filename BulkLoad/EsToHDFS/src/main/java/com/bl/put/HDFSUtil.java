package com.bl.put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Description: ${description}
 * @Date: 2019-03-27 14:10
 */

public class HDFSUtil {
  private static Logger logger = LoggerFactory.getLogger(HDFSUtil.class);
  private static FileSystem hdfsFileSystem = null; //hdfs 文件系统对象

  //初始化 fileSystem 对象
  static {
    try {
      //获取hdfs配置文件(uri地址、用户名)，用什么权限去访问hdfs集群上的文件
      Configuration conf = new Configuration();

      //这里指定使用的是 hdfs文件系统
      conf.set("fs.defaultFS", "hdfs://node2.gh.swc.hdp:8020");

      //通过这种方式设置java客户端身份
      System.setProperty("HADOOP_USER_NAME", "hdfs");
      hdfsFileSystem = FileSystem.get(conf); //这个方式是自动读取hdfs-site.xml
//    URI uri = URI.create("hdfs://10.1.116.20:8020/");   //定义uri
//    FileSystem.get(uri,new Configuration());
    } catch (IOException e) {
      logger.error("hdfs文件系统对象初始化失败!");
      e.printStackTrace();
    }
  }


  /**
   * @param path 文件系统路径
   * @return 该路径文件夹是否存在
   */
  public static boolean isDirectory(String path) throws IOException {
    return hdfsFileSystem.isDirectory(new Path(path));
  }

  /**
   * @param path 文件系统路径
   * @return 该路径文件是否存在
   */
  public static boolean isFile(String path) throws IOException {
    return hdfsFileSystem.isFile(new Path(path));
  }

  /**
   * @param path 文件路径
   * @return hdfs 上创建指定Path的文件夹
   */
  public static boolean createDirectory(String path) throws IOException {
//    FsPermission permission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
//    hdfsFileSystem.setPermission(new Path(path),permission);
    return hdfsFileSystem.mkdirs(new Path(path));
  }

  /**
   * 将本地路径下的文件上传到hdfs目录上
   * @param locaPath 本地文件系统的路径
   * @param hdfsPath hdfs文件系统的路径
   * @return
   */
  public static void putFileToHdfs(String locaPath,String hdfsPath) throws IOException {
//    if (!hdfsFileSystem.exists(new Path(hdfsPath)))
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    hdfsFileSystem.copyFromLocalFile(new Path(locaPath),new Path(hdfsPath));
  }
}
