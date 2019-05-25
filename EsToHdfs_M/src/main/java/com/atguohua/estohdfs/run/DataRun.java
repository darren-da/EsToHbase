package com.atguohua.estohdfs.run;


import com.atguohua.estohdfs.bean.Item;
import com.atguohua.estohdfs.bean.Log;
import com.atguohua.estohdfs.queryData.DayManyQueryData;
import com.atguohua.estohdfs.utils.CreateFileUtil;
import com.atguohua.estohdfs.utils.HDFSUtil;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author :YuFada
 * @date： 2019/5/23 0023 下午 20:16
 * Description：
 * 数据存储的主逻辑，将查找到数据存储到mysql中
 */
@Component
public class DataRun {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(DataRun.class);
    @Value("${spring.table.exception}")
    private String exceptionTable;

    @Value("${spring.table.novalue}")
    private String novalueTable;

    @Value("${spring.table.nocreate}")
    private String nocreateTable;

    @Value("${spring.table.errorTags}")
    private String errorTagsTable;

    @Value("${spring.table.log}")
    private String logTable;

    @Value("${localFilePath}")
    private String localFilePath;

    @Value("${hdfsFilePath}")
    private String hdfsFilePath;

    @Value("${hdfsUrl}")
    String hdfsUrl;

    @Autowired
    private DayManyQueryData esData;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String startFormatTime = "";
    private String endFormatTime = "";

    public void sendData(String plant, String startTime, String endTime) {
        try {
            //1 、配置信息加载完成，从mysql中查找相应的tag点到时序库进行遍历查询，按照一定的条件查询测点数据信息
            //根据厂名，测出一个厂的全部测点
            //2.查询当前 电厂(2713为电厂编号),返回该电厂所有测点 tag 例如:
            String selectSql = "SELECT original_name as Tag FROM gh_dev_pi_b WHERE regulatory_agency=" + plant; //全厂测点
            //3.将查询到的测点 mysql中的数据封装到List中
            List<Map<String, Object>> tagList =
                    jdbcTemplate.queryForList(selectSql);
            //4.将配置文件中的初始时间按照天进行拆分
            ArrayList<Object> days = getDays(startTime, endTime);
            //5.打印本次操作的总日志信息
            logger.info("电厂编号：" +plant + " 本次查询天数:" + days.size()+"天" + " 查询测点数为:" +tagList.size()+"个测点");
            // 时间格式 2018-01-01
            String year = startTime.substring(0, 4);
            //创建本地文件路径
            CreateFileUtil.createDirectory(localFilePath + "/" + year);
            logger.info("相对路径下" + year + "目录已经创建完成！！！");
            //创建hdfs上的文件路径
            if (!HDFSUtil.createDirectory(hdfsFilePath + "/" + year)) {
                logger.info("HDFS 上文件目录" + year + "创建失败!");
                logger.info("HDFS 上文件目录" + year + "已经创建！");


            }
            //  按照天将所有逻辑的数据存到相应的 mysql中
            days.forEach(strDay-> executeEsToMysql(plant,startTime,endTime,tagList, (String) strDay,year));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 将数据存入mysql中
     * @param plant
     * @param startTime
     * @param endTime
     * @param tagList
     * @param strDay
     * @param year
     */
    private void executeEsToMysql(String plant, String startTime, String endTime, List<Map<String, Object>> tagList, String strDay, String year) {
        try {
            Map<String, Object> mapMessage = esData.getEsData(plant, tagList, Collections.singletonList(strDay), year, hdfsFilePath);
            //成功的数据条数
            long success = (long) mapMessage.get("success");//原来是int类型 存不下了
            //失败的数据条数
            long fail = (long) mapMessage.get("fail");
            //无效的tag个数、若计算需要乘以 timeIntervalNumber
            int allFailTagCount = (int) mapMessage.get("allFailTagCount");
            int allNoValueTagCount = (int) mapMessage.get("allNoValueTagCount");
            int allNoCreateTagCount = (int) mapMessage.get("allNoCreateTagCount");

            //需要重新运行的数据的 plant、tags、starttime、endtime、
            List<Item> exceptionMessages = (List<Item>) mapMessage.get("exceptionMessages");
            List<Item> noValuemessages = (List<Item>) mapMessage.get("novalueMessages");
            List<Item> nocreateMessages = (List<Item>) mapMessage.get("nocreateMessages");
            List<Item> errorTagsMessages = (List<Item>) mapMessage.get("errorTagsMessages");


            //2.获取当前时间(毫秒数)
            long start = System.currentTimeMillis();
            startFormatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(start);
            endFormatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());

            failTagsToMysql(exceptionMessages, exceptionTable);
            failTagsToMysql(noValuemessages, novalueTable);
            failTagsToMysql(nocreateMessages, nocreateTable);
            failTagsToMysql(errorTagsMessages,errorTagsTable);

            Long selectTime = new Long(mapMessage.get("selectTime").toString());

            logger.info("开始执行 mapreduce 任务,文件路径:" + hdfsFilePath + "/" + year + " ，plant:" + plant);
            long end = System.currentTimeMillis() - start;
            //将log日志信息写入mysql
            Log log = new Log(plant, startFormatTime, endFormatTime, startTime, endTime, end + "", selectTime + "", (end - selectTime ) + "",  "0", success + "", fail + "", allNoValueTagCount + "",allNoCreateTagCount+"", allFailTagCount + "",tagList.size() +"");
            ArrayList<Log> logArrayList = new ArrayList<>();
            logArrayList.add(log);
            logToMysql(logArrayList, logTable);

            logger.info("电厂:" + plant
                    + " ,测点数:" + tagList.size()
                    + " ,任务开始时间:" + startFormatTime
                    + " ,任务结束时间:" + endFormatTime
                    + " ,数据开始时间:" + startTime
                    + " ,数据结束时间:" + endTime
                    + " ,总耗时(毫秒) " + end
                    + " ,其中查询数据耗时(毫秒) " + selectTime
                    + " ,数据转换时间耗时(毫秒) " + (end - selectTime)
                    + ",数据写入HBase耗时(毫秒) 0"
                    + ",成功数:" + success
                    + ",失败数:" + fail
                    + ",无值的tag数:" + allNoValueTagCount
                    + ",当前时间未创建的tag数:" + allNoCreateTagCount
                    + ",失败的tag数:" + allFailTagCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 将失败的plant、tag、starttime、endtime存入mysql中
     *
     * @param messages List数组，其中每个对象为Item(plant、tag、starttime、endtime)
     */
    private void failTagsToMysql(List<Item> messages, String table) {
        if (messages != null && !messages.isEmpty()) {
            String insertSql = "insert " + table + " (plant,project_start_time,project_end_time,data_start_time,data_end_time,tag,tag_count,plants_size) values (?,?,?,?,?,?,?,?)";
            jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {

                @Override
                public int getBatchSize() {
                    return messages.size();
                }

                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Item item = messages.get(i);
                    ps.setString(1, item.getPlant());
                    ps.setString(2, startFormatTime);
                    ps.setString(3, endFormatTime);
                    ps.setString(4, item.getData_start_time());
                    ps.setString(5, item.getData_end_time());
                    ps.setString(6, item.getTag());
                    ps.setString(7, item.getTag_count());
                    ps.setString(8, item.getPlants_size());
//          ps.setString(5, item.getDatayear());
                }
            });
        }
    }

    /**
     * 任务结束时将日志插入到mysql表中
     *
     * @param messages List数组，其中每个对象为Item(plant、tag、starttime、endtime)
     */
    private void logToMysql(List<Log> messages, String table) {
        if (messages != null && !messages.isEmpty()) {
            String insertSql = "insert " + table + " (plant,project_start_time,project_end_time,data_start_time,data_end_time,all_time,select_time,invert_time,put_hbase_time,success,fail,no_value_count,fail_tag_count,tags_size,no_create_count) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {

                @Override
                public int getBatchSize() {
                    return messages.size();
                }

                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Log log = messages.get(i);
                    ps.setString(1, log.getPlant());
                    ps.setString(2, startFormatTime);
                    ps.setString(3, endFormatTime);
                    ps.setString(4, log.getData_start_time());
                    ps.setString(5, log.getData_end_time());
                    ps.setString(6, log.getAll_time());
                    ps.setString(7, log.getSelect_time());
                    ps.setString(8, log.getInvert_time());
                    ps.setString(9, log.getPut_hbase_time());
                    ps.setString(10, log.getSuccess());
                    ps.setString(11, log.getFail());
                    ps.setString(12, log.getNo_value_count());
                    ps.setString(13, log.getFail_tag_count());
                    ps.setString(14, log.getTags_size());
                    ps.setString(15, log.getNo_create_count());

//          ps.setString(5, item.getDatayear());
                }
            });
        }
    }




    /**
             * 将配置文件中的初始时间按照天进行拆分
             *
             * @param startTime
             * @param endTime
             * @return
             */
            public ArrayList<Object> getDays (String startTime, String endTime) throws Exception {
                ArrayList<Object> date = new ArrayList<>();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date startDate = sdf.parse(startTime);
                Date endDate = sdf.parse(endTime);
                date.add(sdf.format(startDate));
                Calendar calBegin = Calendar.getInstance();
                //使用给定的Date 设置此Calendar 的时间
                calBegin.setTime(startDate);
                Calendar calEnd = Calendar.getInstance();
                //使用给定的Date 设置Calendar 的时间
                calEnd.setTime(endDate);
                //测试此日期是否在指定日期之后
                while (endDate.after(calBegin.getTime())) {
                    //根据日历的规则，给定的日历字段添加或减去指定的时间量
                    calBegin.add(Calendar.DAY_OF_MONTH, 1);
                    date.add(sdf.format(calBegin.getTime()));
                }
                return date;


            }

        }
