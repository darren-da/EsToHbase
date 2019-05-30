package com.atguohua.hd.hdfs.mv;

import com.atguohua.hd.hdfs.HDFSFileSystemUtil;
import com.atguohua.hd.hdfs.mv.bean.FileBean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Description: 得到指定路径下的所有文件名
 * @Date: 2019-05-23 9:54
 */
@Component
public class GetPathFileName {
  @Autowired
  private HDFSFileSystemUtil hdfsFileSystemUtil;

  /**
   * @param initPath hdfs上文件路径
   * @return 得到指定路径下的文件名 HashMap<String, ArrayList<FileBean>>
   * @throws IOException
   */
  public HashMap<String, ArrayList<FileBean>> getPathFileName(String initPath) throws IOException {
    //1.获取hdfs文件系统对象
    FileSystem hdfsFileSystem = hdfsFileSystemUtil.getHdfsFileSystem();
    //2.如果hdfs上的bulkload目录不存在则创建
    if (!hdfsFileSystem.exists(new Path(initPath))) {
      hdfsFileSystem.mkdirs(new Path(initPath));
    }
    //3.得到files的遍历器
    RemoteIterator<LocatedFileStatus> filesIterator = hdfsFileSystem.listFiles(new Path(initPath), false);
    HashMap<String, ArrayList<FileBean>> yearHash = new HashMap<>();
    //4.遍历文件夹,得到其所有文件名
    while (filesIterator.hasNext()) {
      LocatedFileStatus fileStatus = filesIterator.next();
      String fileName = fileStatus.getPath().getName();
      String filePath = fileStatus.getPath().toString();
      String year = fileName.substring(5, 9);
      if (yearHash.get(year) == null) {
        yearHash.put(year, new ArrayList<FileBean>());
      }
      yearHash.get(year).add(new FileBean(fileName, filePath.substring(filePath.indexOf(initPath))));
    }
    //5.如果文件的年份有两个,则到了年份交会的日子,只扫出小年份的文件
    return yearHash;
  }
}
