package com.hadoop.yi.mr.rjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * 在 reducer 端进行合并 （join）
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 1准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        // 2 准备bean对象
        TableBean pdBean = new TableBean();
        for (TableBean bean : values) {
            if ("order".equals(bean.getFlag())) {
                //订单表数据封装
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            } else {
                // 产品表
                try {
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 表的拼接,reduce join;
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            // 数据写出
            context.write(orderBean, NullWritable.get());
        }

    }
}
