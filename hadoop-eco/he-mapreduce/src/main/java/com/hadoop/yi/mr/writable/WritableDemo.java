package com.hadoop.yi.mr.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现自定义bean序列化步骤
 * 1、必须实现 Writable接口
 * 2、反序列化时，需要反射调用空参构造函数
 * 3、重写序列化方法
 * 4、重写反序列化方法
 * 5、反序列化的顺序和序列化的顺序完全一致
 * 6、结果显示在文件中，需要重写 toString(),"\t"分隔
 * 7、自定义bean传在分布式系统中作为key传输，
 * 需要实现Comparable接口，因为MR中Shuffle过程中要求对key必须排序。
 */
public class WritableDemo implements Writable, Comparable<WritableDemo> {
    private long time;
    private String address;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }


    public WritableDemo(long time, String address) {
        super();
        this.time = time;
        this.address = address;
    }

    public WritableDemo() {
        super();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(time);
        out.writeUTF(address);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.time = in.readLong();
        this.address = in.readUTF();
    }

    @Override
    public String toString() {
        return "WritableDemo{" +
                "time=" + time +
                ", address='" + address + '\'' +
                '}';
    }

    @Override
    public int compareTo(WritableDemo o) {
        return this.time > o.getTime() ? -1 : 1;
    }

}
