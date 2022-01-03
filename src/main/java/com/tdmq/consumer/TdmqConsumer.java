//package com.tdmq.consumer;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.pulsar.client.api.*;
//import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import javax.annotation.PostConstruct;
//import javax.annotation.PreDestroy;
//import java.util.concurrent.Executors;
//
///**
// * @Author julyWhj
// * @Description 消费者$
// * @Date 2022/1/2 10:13 上午
// **/
//@Slf4j
//@Service
//public class TdmqConsumer {
//    @Autowired
//    private PulsarClient pulsarClient;
//    private Consumer<byte[]> consumer;
//
//    @PostConstruct
//    public void initConsumer() throws PulsarClientException {
//        log.info("MessageLoggingListener is start");
//        consumer = pulsarClient.newConsumer().topic("pulsar-jm95pr3kmme7/test_juxingta/message_logging").subscriptionName("message_logging_es").subscriptionType(SubscriptionType.Exclusive)
//                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
//                .subscribe();
//        Executors.newSingleThreadExecutor().submit(this::consumer);
//    }
//
//    /**
//     * 销毁容器
//     *
//     * @throws PulsarClientException
//     */
//    @PreDestroy
//    public void destroy() throws PulsarClientException {
//        consumer.close();
//        pulsarClient.close();
//    }
//
//    public void consumer() {
//        while (true) {
//            Message<byte[]> message = null;
//            try {
//                //使用while(true)无问题，该处会发生阻塞释放CPU资源
//                message = consumer.receive();
//                String json = new String(message.getData());
//                if (StringUtils.isNotEmpty(json)) {
//                    log.info("获取消息数据内容：{}", json);
//                }
//                consumer.acknowledge(message);
//            } catch (PulsarClientException e) {
//                log.error("数据消费失败", e);
//            }
//        }
//    }
//}
