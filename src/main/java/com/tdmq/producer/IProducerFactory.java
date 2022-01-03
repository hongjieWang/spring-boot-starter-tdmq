package com.tdmq.producer;


import org.apache.pulsar.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Map;

/**
 * @Author julyWhj
 * @Description 生产者工程接口$
 * @Date 2021/12/30 7:55 下午
 **/
public interface IProducerFactory {
    Map<String, ImmutablePair<Class<?>, String>> getTopics();
}
