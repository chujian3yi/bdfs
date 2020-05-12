package com.hadoop.yi.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class GetJsonObject extends UDF {

    /**
     * 解析json并返回对应的值
     * @param jsonStr
     * @param objName
     * @return
     * @throws JSONException
     */
    public String evaluate(String jsonStr,String objName) throws JSONException {
        if (StringUtils.isBlank(jsonStr) || StringUtils.isBlank(objName)){
            return null;
        }
        JSONObject jsonObject = new JSONObject(new JSONTokener(jsonStr));
        Object objValue = jsonObject.get(objName);
        if (objValue == null){
            return null;
        }
        return objValue.toString();
    }
}
