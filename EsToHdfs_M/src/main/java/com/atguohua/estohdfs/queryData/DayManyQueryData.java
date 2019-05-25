package com.atguohua.estohdfs.queryData;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguohua.estohdfs.bean.Item;
import com.atguohua.estohdfs.bean.ParamEnum;
import com.atguohua.estohdfs.run.InsertMysqlSucessTags;
import com.atguohua.estohdfs.utils.CreateFileUtil;
import com.atguohua.estohdfs.utils.HDFSUtil;
import com.atguohua.estohdfs.utils.HttpClientUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author :YuFada
 * @date： 2019/5/24 0024 上午 9:59
 * Description：
 * <p>
 * 具体实现的业务逻辑
 */
@Service
public class DayManyQueryData {
    public static Logger logger = LoggerFactory.getLogger(DayManyQueryData.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;
    //hbase暂时分开写


    @Value("${pi.requestNum}")
    private int requestNum;
    @Value("${pi.bulk}")
    private int bulk;
    @Value("${pi.timeIntervalNumber}")
    private int timeIntervalNumber;

    @Value("${hbase.url}")
    private String url;

    @Value("${localFilePath}")
    private String localFilePath;
    //统计计算耗时
    long selectStartTime = 0;
    long putHbaseStartTime = 0;
    long selectTime = 0;  //接口查询时间总耗时

    /**
     * @param plant        电厂编号
     * @param tagList      该电厂所有测点组成的tagList集合
     * @param days         传入时间与传出时间按照每天为单位划分成的List
     * @param year         年份
     * @param hdfsFilePath hdfs文件路径
     * @return
     * @throws Exception
     */
    public Map<String, Object> getEsData(String plant, List<Map<String, Object>> tagList, List<String> days, String year, String hdfsFilePath) throws Exception {
        //最终返回的结果集
        Map<String, Object> mapMessage = new HashMap<String, Object>();
        // 对比hbase统计的条数
        long success = 0;  //成功条数
        long fail = 0;  //失败条数
        int allFailTagCount = 0;  //失败的tag条数
        int allNoValueTagCount = 0;  //无值的tag条数
        int allNoCreateTagCount = 0;  //创建失败的tag条数
        int allErrorTagCount = 0;//错误的tag条数

        //存入mysql的表的List （将相信信息的表存入到集合中，最后处理后将数据存入mysql中）
        List<Item> exceptionMessages = new ArrayList<Item>();//查询出现异常的tags ,无法解析
        List<Item> novalueMessages = new ArrayList<Item>(); //向前找两年无值的tag
        List<Item> nocreateMessages = new ArrayList<Item>(); //查询的该节点在当前时间还没有创建
        List<Item> errorTagsMessages = new ArrayList<Item>();//查询到错误的测点，相应的测点数据异常


        //存储hbase的数据集合
        List<Put> list2 = new ArrayList<Put>();

        // 对mysql查询出的测点tagList进行初始化
        List<String> listGroup = new ArrayList(); //查询测点
        List<String> tagsArrayList = new ArrayList<String>(); //将List<Map<String, Object>>中的tags取出并拆分
        for (int i = 1; tagList != null && i <= tagList.size(); i++) {
            Map<String, Object> map = tagList.get(i - 1);
            tagsArrayList.add((String) map.get("Tag"));
            //2.每200个放成一个tags，将其放入listGroup中
            if (i % bulk == 0 && !tagsArrayList.isEmpty() || i == tagList.size()) {
                listGroup.add(StringUtils.join(tagsArrayList.toArray(), ","));
                tagsArrayList = new ArrayList<String>();
            }
        }

        logger.info("分批次查询:" + listGroup.size());

        //按天去查询
        //2019-01-02
        for (String day : days) {
            int batch = 1;
            //分批次去查询
            for (String tags : listGroup) {
                String startTime = day + "T00:00";
                String endTime = day + "T23:59";
                //填入参数
                Map<String, String> params = new HashMap<String, String>();
                params.put(ParamEnum.tags.name(), tags);
                params.put(ParamEnum.plants.name(), plant);
                params.put(ParamEnum.interval.name(), "60");
                params.put(ParamEnum.maxcount.name(), "288000");
                params.put(ParamEnum.starttime.name(), startTime);
                params.put(ParamEnum.endtime.name(), endTime);

                //循环查询测试
                for (int i = 0; i < requestNum; i++) {
                    boolean failTagsIsNull = true;
                    // 统计查询时间耗时
                    selectStartTime = System.currentTimeMillis();
                    String jsonStr = HttpClientUtils.execute(url, params);
                    selectTime += (System.currentTimeMillis() - selectStartTime);

                    boolean isJSONStr = true;  //接口返回的数据是否是JSON类型
                    try {  //是JSON字符串并且能够切分成 getHisAndRealSpanValue
                        JSON.parseObject(jsonStr).getString("getHisAndRealSpanValue").split("\n");
                    } catch (Exception e) {
                        isJSONStr = false;
                    }
                    //此次查询返回的数据是符合${hbase.beatch}JSON格式的字符串
                    if (!"".equals(jsonStr) && isJSONStr) {
                        JSONObject data = JSON.parseObject(jsonStr);
                        String[] items = data.getString("getHisAndRealSpanValue").split("\n");
                        int size = data.getIntValue("size");
                        String failTags = data.getString("failTags");
                        String noDataTags = data.getString("noDataTags");
                        String noCreateTags = data.getString("noFindTags");
                        //新加errorTags 错误的Tags
                        String errorTags = data.getString("errorTags");
                        int state = data.getInteger("state");
                        int itemsLength = items.length;
                        if (data.getString("getHisAndRealSpanValue").equals("")) {
                            itemsLength = 0;
                        }
                        if (state == 1 && itemsLength == size) {  //数据格式正确
                            success += size;
                            //*************start 各个测点的对应数据存入mysql中************

                            // ==== start 取出所有测点，将其他测点过滤掉后留下成功的测点并将相关信息导入mysql中
                            String[] allTagListArray = tags.split(",");
                            HashMap<String, String> hashTag = new HashMap<>();
                            for (String tag : allTagListArray) {
                                hashTag.put(tag, tag);
                            }
                            if (!"".equals(failTags)) {
                                String[] failTagsArray = failTags.split(",");
                                for (String failTag : failTagsArray) {
                                    hashTag.remove(failTag);
                                }
                            }
                            if (!"".equals(noDataTags)) {
                                String[] noDataTagsArray = noDataTags.split(",");
                                for (String noDataTag : noDataTagsArray) {
                                    hashTag.remove(noDataTag);
                                }
                            }
                            if (!"".equals(noCreateTags)) {
                                String[] noCreateTagsArray = noCreateTags.split(",");
                                for (String noCreateTag : noCreateTagsArray) {
                                    hashTag.remove(noCreateTag);
                                }
                            }
                            if (!"".equals(errorTags)) {
                                String[] errorTagsArray = errorTags.split(",");
                                for (String errorTag : errorTagsArray) {
                                    hashTag.remove(errorTag);
                                }
                            }
                            Iterator<String> tagIterator = hashTag.keySet().iterator();
                            ArrayList<String> successTagArray = new ArrayList<>();
                            while (tagIterator.hasNext()) {
                                successTagArray.add(tagIterator.next());
                            }

                            // ==== end
                            for (String tag : successTagArray) {

                                InsertMysqlSucessTags into = new InsertMysqlSucessTags();
                                into.setPlant(plant);
                                into.setTime(day);
                                into.setTag(tag);
                                into.setEs(success + "");
                                into.setTagsize(String.valueOf(tagList.size()));
                                String sql = "INSERT INTO `pi_es_count` (`plant`, `time`, `tag`, `es`,`tag_size`) VALUES (?, ?, ?, ?, ?);";
                                int result = this.jdbcTemplate.update(sql, into.getPlant(), into.getTime(), into.getTag(), 1440, into.getTagsize());
                                if (!(result > 0)) {
                                    throw new RuntimeException("插入mysql失败");
                                }
                            }
                            //*************end 测点数据已经存入到mysql中 ************ 之前设想创建
                            /*InsertMysqlSucessTags into = new InsertMysqlSucessTags();
                            into.setPlant(plant);
                            into.setTime(day);
                            into.setTag(tags);
                            into.setEs(success + "");
                            into.setEs_hbase("");
                            into.setPi("");
                            String sql = "INSERT INTO `pi_es_count` (`plant`, `time`, `tag`, `es`) VALUES (?, ?, ?, ?);";
                            int result = this.jdbcTemplate.update(sql, into.getPlant(), into.getTime(), into.getTag(), into.getEs());
                            if (!(result > 0)) {
                                throw new RuntimeException("插入mysql失败");
                            }*/
                            //*************end************
                            if (!"".equals(noDataTags)) {
                                novalueMessages.add(new Item(plant, startTime, endTime, noDataTags, noDataTags.split(",").length + "", tagList.size() + ""));
                                allNoValueTagCount += noDataTags.split(",").length;
                            }
                            if (!"".equals(noCreateTags)) {
                                nocreateMessages.add(new Item(plant, startTime, endTime, noCreateTags, noCreateTags.split(",").length + "", tagList.size() + ""));
                                allNoCreateTagCount += noCreateTags.split(",").length;
                            }
                            //错误的tags数
                            if (!"".equals(errorTags)) {
                                errorTagsMessages.add(new Item(plant, startTime, endTime, errorTags, errorTags.split(",").length + "", tagList.size() + ""));
                            }
                            //对失败的failTags进行判断,如果没有到达失败次数,则循环遍历failTags
                            if (!failTags.isEmpty()) {
                                if (i == requestNum - 1) {
                                    allFailTagCount += failTags.split(",").length;
                                    exceptionMessages.add(new Item(plant, startTime, endTime, failTags, failTags.split(",").length + "", tagList.size() + ""));
                                } else {
                                    params.remove(ParamEnum.tags.name());
                                    params.put(ParamEnum.tags.name(), failTags);
                                }
                                failTagsIsNull = false;
                            }
                            if (!data.getString("getHisAndRealSpanValue").equals("")) {
                                CreateFileUtil.createTxtFile(items, localFilePath + "/" + year, plant + "-" + day + ".txt"); //直接将返回的数据存储为一行数据
                            }
                        } else {//接口返回的数据可以解析,但是数据格式不正确。此时接口不正确,重试
                            Thread.sleep(1000);
                            if (i % 5 == 10 && i != 0) {
                                logger.info("接口返回的数据可以解析,但是数据格式不正确。正在重试,次数:" + (i + 1) + " ,state:" + state + " ,items.length:" + items.length);
                            }
                            if (i == requestNum - 1) {
                                String thisTags = params.get(ParamEnum.tags.name());
                                fail = thisTags.split(",").length * timeIntervalNumber + fail;
                                allFailTagCount += thisTags.split(",").length;
                                exceptionMessages.add(new Item(plant, startTime, endTime, thisTags, thisTags.split(",").length + "", tagList.size() + ""));
                                logger.error("重试次数到达上限,次数:" + requestNum + " ,state:" + state + " ,items.length:" + items.length + " ,size:" + size);
                            }
                            continue;
                        }
                    } else { //接口返回的数据是空字符串或无法解析
                        Thread.sleep(1000);
                        if (i % 10 == 0 && i != 0) {
                            logger.info("此次返回的数据无法解析。正在重试,次数:" + (i + 1) + ", plant:" + plant + " ,startTime:" + startTime + " ,endTime:" + endTime + " ,tags:" + tags);
                        }
                        if (i == requestNum - 1) {
                            String thisTags = params.get(ParamEnum.tags.name());
                            fail = thisTags.split(",").length * timeIntervalNumber + fail;
                            allFailTagCount += thisTags.split(",").length;
                            exceptionMessages.add(new Item(plant, startTime, endTime, thisTags, thisTags.split(",").length + "", tagList.size() + ""));
                            logger.error("此次返回的数据无法解析。重试次数到达上限,次数:" + requestNum + " ,返回数据 jsonStr:" + jsonStr);
                        }
                        continue;
                    }
                    if (failTagsIsNull) { //此次接口返回数据为正确数据、failTags=0,跳出重试循环
                        break;
                    }
                }
                //for 结束
                logger.info("===== 电厂:" + plant + "===== " + day + " ===== ,第 " + batch++ + " 批次数据插入完成 =====");
            }
            logger.info("===== 电厂:" + plant + " ============ " + day + "插入完成  ============");
            logger.info("成功数:" + success + " ,失败数:" + fail + " ,无值的tag数:" + allNoValueTagCount + " ," +
                    "当前时间未创建的tag数:" + allNoCreateTagCount + " ,失败的tag数:" + allFailTagCount +
                    ",错误的tag数：" + allErrorTagCount);
            //开始将这一天的数据传到hdfs上
            String fileName = "/" + year + "/" + plant + "-" + day + ".txt";
            HDFSUtil.putFileToHdfs(localFilePath + fileName, hdfsFilePath + fileName);
            logger.info("localFilePath:" + localFilePath + " ,hdfsFilePath:" + fileName);
            CreateFileUtil.deleteLocalFile(localFilePath + fileName);
            logger.info("本地路径下 " + localFilePath + fileName + " 已删除！");

            logger.info("##############################");
            logger.info(day + ",的相关数据信息准备存入相应的mysql中");
            jdbcTemplate.update("UPDATE pi_es_count SET success_tag_size='" + (success / 1440) + "' WHERE plant='" + plant + "' AND TIME='" + day + "' and success_tag_size is null");
        }
        mapMessage.put("success", success);
        mapMessage.put("fail", fail);
        mapMessage.put("allFailTagCount", allFailTagCount);
        mapMessage.put("allNoValueTagCount", allNoValueTagCount);
        mapMessage.put("allNoCreateTagCount", allNoCreateTagCount);
        mapMessage.put("allErrorTagCount", allErrorTagCount);
        mapMessage.put("exceptionMessages", exceptionMessages);
        mapMessage.put("novalueMessages", novalueMessages);
        mapMessage.put("nocreateMessages", nocreateMessages);
        mapMessage.put("selectTime", selectTime);
        mapMessage.put("errorTagsMessages", errorTagsMessages);


        return mapMessage;
    }
}