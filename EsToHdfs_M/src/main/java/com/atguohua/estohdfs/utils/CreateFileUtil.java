package com.atguohua.estohdfs.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * @Description: 生成txt文件，行数几乎无上限
 * @Date: 2019-03-27 15:52
 */
public class CreateFileUtil {
  public static Logger logger = LoggerFactory.getLogger(CreateFileUtil.class);

  public static boolean createTxtFile(String[] items, String filePath, String fileName) {
    boolean flag = true; // 标记文件生成是否成功
    try {
      // 含文件名的全路径
      String fullPath = filePath + File.separator + fileName;
      File file = new File(fullPath);
      if (!file.exists()) {
        logger.info(fileName + " 文件已创建！");
      }
      //如果文件存在，则追加内容；如果文件不存在，则创建文件
      PrintWriter printWriter = new PrintWriter(new FileWriter(new File(fullPath), true));
      for (String item : items) {
        printWriter.print(item);
        printWriter.print(",,"); //将每条数据以  ,,  为间隔
      }
      printWriter.println();
      printWriter.flush();
    } catch (Exception e) {
      e.printStackTrace();
      flag = false;
    }
    return flag;
  }

  public static boolean createDirectory(String path) {
    boolean flag = true;
    File directory = new File(path);
    if (!directory.exists()) {
      directory.mkdirs();
    }
    return flag;
  }

  public static boolean deleteLocalFile(String localPath){
    boolean flag = true;
    File file = new File(localPath);
    if (file.isFile() && file.exists()){
      flag = file.delete();
    }
    return flag;
  }

}
