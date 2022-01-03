package com.tdmq.producer;

import com.tdmq.collector.ProducerCollector;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * @Author julyWhj
 * @Description 模版工具类$
 * @Date 2022/1/3 1:42 下午
 **/
public class TdmqTemplate<T> {
    private final ProducerCollector producerCollector;

    public TdmqTemplate(ProducerCollector producerCollector) {
        this.producerCollector = producerCollector;
    }

    /**
     * 发送消息接口
     *
     * @param topic 队列
     * @param msg   消息内容
     * @return
     * @throws PulsarClientException
     */
    public MessageId send(String topic, T msg) throws PulsarClientException {
        return producerCollector.getProducer(topic).send(msg);
    }

    /**
     * 异步发送消息接口
     *
     * @param topic   队列
     * @param message 消息内容
     * @return
     */
    public CompletableFuture<MessageId> sendAsync(String topic, T message) {
        return producerCollector.getProducer(topic).sendAsync(message);
    }

    /**
     * 构建消息
     *
     * @param topic   队列
     * @param message 消息内容
     * @return
     */
    public TypedMessageBuilder<T> createMessage(String topic, T message) {
        return producerCollector.getProducer(topic).newMessage().value(message);
    }
}
