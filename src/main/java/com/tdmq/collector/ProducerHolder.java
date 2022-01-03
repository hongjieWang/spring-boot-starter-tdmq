package com.tdmq.collector;

/**
 * @Author julyWhj
 * @Description 生产者绑定关系$
 * @Date 2022/1/3 1:26 下午
 **/
public class ProducerHolder {
    private final String topic;
    private final Class<?> clazz;
    private final String serialization;

    public ProducerHolder(String topic, Class<?> clazz, String serialization) {
        this.topic = topic;
        this.clazz = clazz;
        this.serialization = serialization;
    }

    public String getTopic() {
        return topic;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public String getSerialization() {
        return serialization;
    }

}
