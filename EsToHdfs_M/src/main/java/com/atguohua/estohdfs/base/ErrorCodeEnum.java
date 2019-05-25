package com.atguohua.estohdfs.base;

/**
 * @author :YuFada
 * @date： 2019/5/12 0012 下午 15:58
 * Description：
 */
public enum ErrorCodeEnum {
    /**
     * 系统类
     */
    SYSTEM_ERROR_REMIND(9998, "为保障您的账户安全，请勿频繁操作"),
    SYSTEM_ERROR(9999, "系统异常，请稍后重试"),
    SYSTEM_PARAMETER_ERROR(7000, "参数异常"),
    SYSTEM_PARAMETER_NOTFOUND(7100, "参数丢失"),
    SYSTEM_PARAMETER_NOT(7220, "参数不能为空"),
    SYSTEM_PARAMETER_NOTRULE(7200, "参数非法"),
    HDFS_INPUTURL_NOT_FOUND(9527, "输入路基机哪个不穿在");


    private int code;
    private String message;

    ErrorCodeEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return this.code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static boolean contain(Integer value) {
        if (value == null) {
            return false;
        }
        ErrorCodeEnum[] values = values();
        for (ErrorCodeEnum sexEnum : values) {
            if (sexEnum.code == value.intValue()) {
                return true;
            }
        }
        return false;
    }
}
