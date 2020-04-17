package com.hadoop.yi.mr.ordergc;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer,需求订单中最贵的商品，使用辅助排序将订单id相同的kv聚合成组，取第一个即时订单中最贵的商品
 */
public class OrderSortReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
