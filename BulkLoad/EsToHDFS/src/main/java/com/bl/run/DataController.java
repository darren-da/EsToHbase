package com.bl.run;

import com.bl.put.HDFSUtil;
import com.bl.source.bean.Item;
import com.bl.source.bean.Log;
import com.bl.source.file.CreateFileUtil;
import com.bl.source.invert.DayManyQueryData;
import com.google.protobuf.TextFormat.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class DataController {

  public static Logger logger = LoggerFactory.getLogger(DataController.class);

  @Value("${pi.plant}")
  private String plant;

  @Value("${pi.startTime}")
  private String startTime;

  @Value("${pi.endTime}")
  private String endTime;

  @Value("${spring.table.exception}")
  private String exceptionTable;

  @Value("${spring.table.novalue}")
  private String novalueTable;

  @Value("${spring.table.nocreate}")
  private String nocreateTable;

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

  @PostConstruct
  public void sendData() {

    //1.配置信息加载完成
    try {
      //2.获取当前时间(毫秒数)
      long start = System.currentTimeMillis();
      //3.查询当前 电厂(2713为电厂编号),返回该电厂所有测点 tag 例如:
      String selectSql = "SELECT original_name as Tag FROM gh_dev_pi_b WHERE regulatory_agency=" + plant; //全厂测点
//      String selectSql = "select tag as Tag from test_hbase_yinfengji where regulatory_agency=" + plant; //引风机测点
//      String selectSql = "select a.original_name as Tag from test_gh_dev_pi_b a left join test_hbase_yinfengji b on a.regulatory_agency = b.regulatory_agency and a.original_name = b.tag where a.regulatory_agency = "+plant+" and b.tag is null"; //全厂除去引风机测点


      //4.将mysql表中的数据封装到List中
      List<Map<String, Object>> tagList = jdbcTemplate.queryForList(selectSql);
      //5.将配置文件中的初始时间和结束时间按照一天为单位进行拆分
      List<String> days = getDays(startTime, endTime);
      //6.打印本次操作的总日志
      logger.info("电厂:" + plant + " 查询天数:" + days.size() + " 查询测点数:" + tagList.size());

      String year = startTime.substring(0, 4);

      CreateFileUtil.createDirectory(localFilePath + "/" + year);
      logger.info("相对路径下 " + year + " 目录已创建！");

      // 创建HDFS文件夹
      if (!HDFSUtil.createDirectory(hdfsFilePath + "/" + year)){
        logger.info("HDFS 上文件目录 " + year + " 创建失败！");
      }else {
        logger.info("HDFS 上文件目录 " + year + " 已创建！");
      }

      //  对hbase进行数据操作，将其写入到本地文件路径下
      Map<String, Object> mapMessage = esData.getEsData(plant, tagList, days, year,hdfsFilePath);
      //成功的数据条数
      long success = (long) mapMessage.get("success");
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

      startFormatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(start);
      endFormatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis());

      failTagsToMysql(exceptionMessages, exceptionTable);
      failTagsToMysql(noValuemessages, novalueTable);
      failTagsToMysql(nocreateMessages, nocreateTable);

      Long selectTime = new Long(mapMessage.get("selectTime").toString());

      logger.info("开始执行 mapreduce 任务,文件路径:" + hdfsFilePath + "/" + year + " ，plant:" + plant);
      long end = System.currentTimeMillis() - start;
      //将log日志信息写入mysql
      Log log = new Log(plant, startFormatTime, endFormatTime, startTime, endTime, end + "", selectTime + "", (end - selectTime ) + "",  "0", success + "", fail + "", allNoValueTagCount + "",allNoCreateTagCount+"", allFailTagCount + "", tagList.size() + "");
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
      // TODO Auto-generated catch block
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
   * 按天拆分
   *
   * @throws ParseException
   * @throws java.text.ParseException
   */

  public List<String> getDays(String startTime, String endTime) throws Exception {
    List<String> date = new ArrayList<String>();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Date startDate = sdf.parse(startTime);
    Date endDate = sdf.parse(endTime);
    date.add(sdf.format(startDate));
    Calendar calBegin = Calendar.getInstance();
    // 使用给定的 Date 设置此 Calendar 的时间
    calBegin.setTime(startDate);
    Calendar calEnd = Calendar.getInstance();
    // 使用给定的 Date 设置此 Calendar 的时间
    calEnd.setTime(endDate);
    // 测试此日期是否在指定日期之后
    while (endDate.after(calBegin.getTime())) {
      // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
      calBegin.add(Calendar.DAY_OF_MONTH, 1);
      date.add(sdf.format(calBegin.getTime()));
    }
    return date;
  }


}
