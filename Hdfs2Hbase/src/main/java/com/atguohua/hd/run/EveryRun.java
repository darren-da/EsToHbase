package com.atguohua.hd.run;


import com.atguohua.hd.hdfs.mv.GetPathFileName;
import com.atguohua.hd.hdfs.mv.MoveDataFileToPath;
import com.atguohua.hd.hdfs.mv.MoveDataUtil;
import com.atguohua.hd.hdfs.mv.bean.FileBean;
import com.atguohua.hd.linux.shell.LinuxShell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @Description: 移动hdfs上的文件操作类
 * @Date: 2019-05-22 17:24
 */
@Component
@Configurable
@EnableScheduling
public class EveryRun {
    private Logger logger = LoggerFactory.getLogger(EveryRun.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private GetPathFileName getPathFileNameObject;
    @Autowired
    private MoveDataUtil moveDataUtilObject;
    @Autowired
    private MoveDataFileToPath moveDataFileToPathObject;
    @Autowired
    private LinuxShell linuxShellObject;

    @Value("${hdfs.path.init}")
    private String initPath;
    @Value("${hdfs.path.bulkload.input}")
    private String inputPath;
    @Value("${hdfs.path.bulkload.output}")
    private String outputPath;
    @Value("${mysql.insertTableDay}")
    private String insertTableDay;
    @Value("${linux.shell}")
    private String linuxShellInitCommand;
    @Value("${linux.tableNamePrefix}")
    private String tableNamePrefix;
    //定时调度，每天夜间1点执行程序将数据导入到hbase中
    //@PostConstruct
    @Scheduled(cron = "0 0 1 * * ?")
    public void everyRun() throws IOException, InterruptedException {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);   //按照第一个参数的类型向前或向后移动时间
        String date = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());  //通过时间转换的秒数来取date

        logger.info("===== date:" + date + " ,定时调度BulkLoad  开始 =====");

        //1.调用方法获取所有文件名
        HashMap<String, ArrayList<FileBean>> fileNameHash = getPathFileNameObject.getPathFileName(initPath);
        Iterator<String> iteratorFileName = fileNameHash.keySet().iterator();

        ArrayList<FileBean> fileBeans = null;
        Integer minYear = null;
        //2.取出最小年份的文件名集合，若为空则返回false
        if (iteratorFileName.hasNext()) {
            minYear = moveDataUtilObject.getMinYear(iteratorFileName);
            fileBeans = fileNameHash.get(String.valueOf(minYear));
        } else {
            //由于文件夹为空,直接返回false
            logger.info("===== date:" + date + " ,文件夹为空 =====");
            return;
        }

        //3.循环遍历文件,将其移动到指定路径下
        moveDataFileToPathObject.moveDataFileToPath(fileBeans, inputPath, outputPath);

        //4.使用java远程调用shell执行bulkload程序
        //参数:linux的初始shell、inputPath、outPutPath、表名、更新mysql表的表名、存入mysql的时间
        linuxShellObject.linuxShell(linuxShellInitCommand, inputPath, outputPath, tableNamePrefix + minYear, date);

        /**
         * 6.根据create_time监听表看 run_status 是否是3
         * run_status : 1 在此程序还未到bulkload
         * run_status : 2 bulkload即将进行
         * run_status : 3 bulkload进行完成,此程序开始删除outPutPath路径文件夹
         */


        logger.info("===== date:" + date + " ,定时调度BulkLoad  完成 =====");
    }
    //bulkload完成后,删除bulkload的输入路径下文件，bulkload.input:/hbase-data/test-input/*
    //和删除  bulkload完成后生成的  Hfile文件，bulkload.output: /hbase-data/test-output,
    // 注意：移动文件前已经完成操作


}
