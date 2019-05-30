package com.atguohua.hd.hdfs.mv;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Description: 移动hdfs上文件的Util类
 * @Date: 2019-05-23 10:41
 */
@Component
public class MoveDataUtil {

  /**
   * 将传入的遍历器中取出最小值
   * @param iterator
   */
  public Integer getMinYear(Iterator<String> iterator){
    ArrayList<Integer> yearList = new ArrayList<>();
    while (iterator.hasNext()) {
      yearList.add(Integer.parseInt(iterator.next()));
    }
    return Collections.min(yearList);
  }
}
