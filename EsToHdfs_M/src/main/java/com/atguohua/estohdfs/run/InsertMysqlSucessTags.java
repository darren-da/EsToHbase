package com.atguohua.estohdfs.run;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * create by chenjiang on 2019/5/19
 */
@Setter
@Getter
public class InsertMysqlSucessTags implements Serializable {

    /**
     * plantId
     */
    private String plant;
    private String time;
    private String tag;
    private String es;
    private String tagsize;

}