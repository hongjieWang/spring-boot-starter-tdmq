//package com.tdmq.producer;
//
//import org.apache.pulsar.client.api.MessageId;
//import org.apache.pulsar.client.api.Producer;
//import org.apache.pulsar.client.api.PulsarClient;
//import org.apache.pulsar.client.api.PulsarClientException;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
///**
// * @Author julyWhj
// * @Description tdmq生产者$
// * @Date 2022/1/2 9:57 上午
// **/
//@Service
//public class TdmqProucer {
//    @Autowired
//    private PulsarClient pulsarClient;
//
//    /**
//     * 发送消息
//     *
//     * @param message 消息内容
//     * @return 消息ID
//     * @throws PulsarClientException
//     */
//    public MessageId sendMsg(String topic, String message) throws PulsarClientException {
//        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
//        MessageId messageId = producer.newMessage().value(message.getBytes()).send();
//        producer.close();
//        return messageId;
//    }
//}
