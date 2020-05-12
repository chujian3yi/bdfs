package com.hadoop.yi.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class YiLower extends UDF {

    /**
     * 求输入字符串的字母小写值
     * @param s
     * @return
     */
    public String evaluate (final String s){
     if (s == null){
         return null;
     }
     return s.toLowerCase();
    }
}


