package com.hadoop.yi.flume.sources;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    // 定义配置文件将来要读取的字段
    private Long delay;
    private String field;



    /**
     * 初始化配置信息
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field","Hello!");
    }

    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
