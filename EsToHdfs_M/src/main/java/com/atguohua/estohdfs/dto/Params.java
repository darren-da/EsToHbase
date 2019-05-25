package com.atguohua.estohdfs.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * @author :YuFada
 * @date： 2019/5/23 0023 下午 20:08
 * Description：
 */

/**
 * 发布接口时需要传入的参数
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Component
public class Params {
    private String plant;
    private String startTime;
    private String endTime;
    private String url;


}
