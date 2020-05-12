package com.hadoop.yi.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONTokener;

import java.util.ArrayList;

public class GetJsonArray extends UDF {

    /**
     * 解析json数组字符串并返回对应子json字符串数组
     * @param jsonArrayStr
     * @return
     */
    public static ArrayList<Text> evaluate(String jsonArrayStr) throws JSONException {
        if (StringUtils.isBlank(jsonArrayStr)){
            return null;
        }
        ArrayList<Text> textList = new ArrayList<Text>();
        if (!jsonArrayStr.trim().startsWith("[")){
            textList.add(new Text(jsonArrayStr));
        }else {
            JSONArray jsonArray = new JSONArray(new JSONTokener(jsonArrayStr));
            for (int i = 0; i < jsonArray.length(); i++) {
                String json = jsonArray.get(i).toString();
                textList.add(new Text(json));
            }
        }
        return textList;
    }

    public static void main(String[] args) throws JSONException {
        String jsonStr = "{ \"name\": \"mady\", \"sex\": \"man\" }";
                ArrayList<Text> evaluate = evaluate(jsonStr);
        System.out.println(evaluate);
    }
}
