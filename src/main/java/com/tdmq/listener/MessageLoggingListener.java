package com.tdmq.listener;

import com.tdmq.annotation.TdmqConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.shade.org.apache.avro.data.Json;
import org.springframework.stereotype.Service;

/**
 * @Author julyWhj
 * @Description 消息队列消费者$
 * @Date 2022/1/3 2:01 下午
 **/
@Slf4j
@Service
public class MessageLoggingListener {
    public static final String MESSAGE_LOGGING_TOPIC = "message_logging";

    @TdmqConsumer(topic = MESSAGE_LOGGING_TOPIC, consumerName = "message_logging", clazz = String.class, subscriptionName = "message_logging_es")
    void consume(String msg) {
        log.info("------------{}", msg);
    }
}
