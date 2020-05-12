package com.hadoop.yi.gl.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GlETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text K = new Text();
    private StringBuilder sb = new StringBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String result = handleLine(line);
        if (result == null) {
            context.getCounter("ETL", "False").increment(1);
        } else {
            context.getCounter("ETL", "True").increment(1);
            K.set(result);
            context.write(K, NullWritable.get());
        }
    }

    /**
     * etl方法，处理长度不够的数据，并且转换数据形式
     * 长度不够去掉，类别用"&"分割，去掉两边空格，多个视频id使用"&"分割
     *
     * @param line
     * @return
     */
    private String handleLine(String line) {
        String[] fields = line.split("\t");
        if (fields.length < 9) {
            return null;
        }
        //sb.delete(0,sb.length());
        sb.setLength(0);
        fields[3] = fields[3].replace(" ", "");
        for (int i = 0; i < fields.length; i++) {
            if (i == fields.length - 1) {
                sb.append(fields[i]);
            } else if (i < 9) {
                sb.append(fields[i]).append("\t");
            } else {
                sb.append(fields[i]).append("&");
            }
        }
        return sb.toString();
    }
}
