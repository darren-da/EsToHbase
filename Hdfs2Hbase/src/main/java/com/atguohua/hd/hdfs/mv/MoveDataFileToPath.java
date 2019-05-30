package com.atguohua.hd.hdfs.mv;

import com.atguohua.hd.hdfs.HDFSFileSystemUtil;
import com.atguohua.hd.hdfs.mv.bean.FileBean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Description: 将文件移动到指定的目录下
 * @Date: 2019-05-23 10:48
 */
@Component
public class MoveDataFileToPath {
  @Autowired
  private HDFSFileSystemUtil hdfsFileSystemUtil;

  /**
   * 将扫描到的所有文件移动到bulkload-input的路径
   *
   * @param fileBeans
   * @param inputPath
   * @throws IOException
   */
  public void moveDataFileToPath(ArrayList<FileBean> fileBeans, String inputPath, String outputPath) throws IOException {
    FileSystem hdfsFileSystem = hdfsFileSystemUtil.getHdfsFileSystem();
    Path input = new Path(inputPath);
    if (hdfsFileSystem.exists(input)) {
      hdfsFileSystemUtil.getHdfsFileSystem().delete(input, true);
    }
    if (hdfsFileSystem.exists(new Path(outputPath))) {
      hdfsFileSystemUtil.getHdfsFileSystem().delete(new Path(outputPath), true);
    }
    hdfsFileSystem.mkdirs(input);
    hdfsFileSystem.setPermission(input, new FsPermission("777"));
    for (FileBean fileBean : fileBeans) {
      String fileName = fileBean.getFileName();
      String filePath = fileBean.getFilePath();
      Path newFilePath = new Path(inputPath + "/" + fileName);
      hdfsFileSystem.rename(new Path(filePath), newFilePath);
      hdfsFileSystem.setPermission(newFilePath, new FsPermission("777"));
    }
  }
}
