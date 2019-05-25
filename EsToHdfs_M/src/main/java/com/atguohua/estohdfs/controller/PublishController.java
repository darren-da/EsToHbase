package com.atguohua.estohdfs.controller;

import com.atguohua.estohdfs.base.BaseError;
import com.atguohua.estohdfs.base.BaseResponseResult;
import com.atguohua.estohdfs.base.ErrorCodeEnum;
import com.atguohua.estohdfs.dto.Params;
import com.atguohua.estohdfs.run.DataRun;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * @author :YuFada
 * @date： 2019/5/23 0023 下午 20:07
 * Description：
 */
@RestController
@Slf4j
public class PublishController {
    @Autowired
    private DataRun dataRun;

    @Value("${pi.url}")
    private String url;
    // @Autowired
    //private PIData piData;  参数由前端传入

    // Post请求
    @GetMapping("/getEsData")
    public BaseResponseResult getPIData(@RequestParam(value = "plant", required = false) String plant,
                                        @RequestParam(value = "startTime", required = false) String startTime,
                                        @RequestParam(value = "endTime", required = false) String endTime,
                                        HttpServletRequest request) {
        BaseResponseResult result = new BaseResponseResult();
        try {
            Params params = new Params(plant,startTime,endTime,url);

            result.setData(params);

            dataRun.sendData(plant,startTime,endTime);
        } catch (Exception e) {
            log.error("系统异常");
            result.setError(new BaseError(ErrorCodeEnum.SYSTEM_ERROR.getMessage()));
        }
        return result;
    }



}
