package com.tdmq.producer;

import com.sun.xml.internal.ws.developer.Serialization;
import com.tdmq.annotation.TdmqProducer;
import org.apache.pulsar.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author julyWhj
 * @Description $
 * @Date 2022/1/3 1:27 下午
 **/
@TdmqProducer
public class ProducerFactory implements IProducerFactory {

    private final Map<String, ImmutablePair<Class<?>, String>> topics = new HashMap<>();

    public ProducerFactory addProducer(String topic) {
        return addProducer(topic, byte[].class, "JSON");
    }

    public ProducerFactory addProducer(String topic, Class<?> clazz) {
        topics.put(topic, new ImmutablePair<>(clazz, "JSON"));
        return this;
    }

    public ProducerFactory addProducer(String topic, Class<?> clazz, String serialization) {
        topics.put(topic, new ImmutablePair<>(clazz, serialization));
        return this;
    }

    @Override
    public Map<String, ImmutablePair<Class<?>, String>> getTopics() {
        return topics;
    }
}

