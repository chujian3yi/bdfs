package com.hadoop.yi.mr.ordergc;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 订单信息 bean：id升序排序，金额降序排序
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id; // 订单id 号
    private double price;   // 订单总价

    /**
     * 该构造将比较对象的类传递给父类
     *
     */
    public OrderBean(){super();}

    public OrderBean(int order_id, double price) {
        super();
        this.order_id = order_id;
        this.price = price;
    }

    // 二次排序
    @Override
    public int compareTo(OrderBean o) {
        // 先比较订单id号，再比较订单总价price
        int result;
        if (order_id > o.getOrder_id()){
            result = 1;
        }else if (order_id < o.getOrder_id()){
            result = -1;
        }else{
            // 价格倒排
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        price = in.readDouble();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "order_id=" + order_id +
                ", price=" + price +
                '}';
    }
}