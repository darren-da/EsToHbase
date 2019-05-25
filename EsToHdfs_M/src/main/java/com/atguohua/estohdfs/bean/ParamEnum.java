package com.atguohua.estohdfs.bean;

/**
 * 枚举类
 */
public enum ParamEnum {
	tags,plants,starttime,endtime,interval,maxcount,tokenHeader("Authorization");
	
	String value;
	
	ParamEnum() {
	}
	
	ParamEnum(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
}
