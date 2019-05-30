package com.atguohua.hd.linux.shell;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Description: Linux Shell的主逻辑方法
 * @Date: 2019-05-22 14:01
 */
@Component
public class LinuxShell {
  private Logger logger = LoggerFactory.getLogger(LinuxShell.class);

  public void linuxShell(String linuxShellInitCommand, String inputPath, String outPutPath, String tableName, String date) throws IOException, InterruptedException {
    InputStreamReader stdISR = null;
    InputStreamReader errISR = null;
    Process process = null;

    logger.info("===== Linux Shell开始运行 =====");
    String linuxShellCommand = linuxShellInitCommand + " " + inputPath + " " + outPutPath + " " + tableName;
    try {
      process = Runtime.getRuntime().exec(linuxShellCommand);
      int exitValue = process.waitFor();
      logger.info("===== 发送 shell命令的 hadoop jar bulkload 开始执行 ===== date:" + date);
      logger.info("===============================");
      logger.info(linuxShellCommand);
      logger.info("===============================");

      String line = null;

      stdISR = new InputStreamReader(process.getInputStream());
      BufferedReader stdBR = new BufferedReader(stdISR);
      while ((line = stdBR.readLine()) != null) {
        System.out.println("STD line:" + line);
      }

      errISR = new InputStreamReader(process.getErrorStream());
      BufferedReader errBR = new BufferedReader(errISR);
      while ((line = errBR.readLine()) != null) {
        System.out.println("ERR line:" + line);
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
        if (stdISR != null) {
          stdISR.close();
        }
        if (errISR != null) {
          errISR.close();
        }
        if (process != null) {
          process.destroy();
        }
      } catch (IOException e) {
        System.out.println("正式执行命令：" + linuxShellCommand + "有IO异常");
      }
    }
  }
}
















