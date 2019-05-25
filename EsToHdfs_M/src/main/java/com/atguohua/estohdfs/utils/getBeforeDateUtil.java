package com.atguohua.estohdfs.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/**
 * @author :YuFada
 * @date： 2019/4/24 0024 下午 15:13
 * Description：
 */
public class getBeforeDateUtil {
    private static Logger logger = LoggerFactory.getLogger(getBeforeDateUtil.class);

    public void getDate() {
        Date date = new Date();
        //申明日期格式化样式
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        //将格式化的日期字符串转为Date
        try {
            date = dateFormat.parse(dateFormat.format(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //通过Calendar的实现类获得Calendar实例
        Calendar calendar = Calendar.getInstance();
        //设置格式化的日期
        calendar.setTime(date);
        //获取当前 日期
        int day = calendar.get(Calendar.DATE);
        //获取设置前一天的日期
        calendar.set(Calendar.DATE, day - 1);
        //获取
        String dateStr = dateFormat.format(calendar.getTime());

        logger.info("需要执行查询的日期是：" + dateStr);


    }
}
