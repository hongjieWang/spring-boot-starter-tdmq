package com.tdmq.consumer;

import com.tdmq.collector.ConsumerCollector;
import com.tdmq.collector.ConsumerHolder;
import com.tdmq.config.TdmqProperties;
import org.apache.pulsar.client.api.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author julyWhj
 * @Description 消息处理聚合器$
 * @Date 2022/1/3 11:09 上午
 **/
@Component
@DependsOn({"pulsarClient"})
public class ConsumerAggregator implements EmbeddedValueResolverAware {
    private final ConsumerCollector consumerCollector;
    private final PulsarClient pulsarClient;
    private final static SubscriptionType DEFAULT_SUBSCRIPTION_TYPE = SubscriptionType.Exclusive;
    private final TdmqProperties tdmqProperties;
    private StringValueResolver stringValueResolver;
    private List<Consumer> consumers;

    public ConsumerAggregator(ConsumerCollector consumerCollector, PulsarClient pulsarClient, TdmqProperties tdmqProperties) {
        this.consumerCollector = consumerCollector;
        this.pulsarClient = pulsarClient;
        this.tdmqProperties = tdmqProperties;
    }

    /**
     * 待spring上下文启动完毕后，加载注解init()方法
     */
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        //获取收集器中所有的消费者对象
        consumers = consumerCollector.getConsumers().entrySet().stream()
                .map(holder -> subscribe(holder.getKey(), holder.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * 消费者消息监听处理类
     *
     * @param generatedConsumerName 消费者名称
     * @param holder                绑定关系
     * @return
     */
    private Consumer<?> subscribe(String generatedConsumerName, ConsumerHolder holder) {
        try {
            //从注解中获取消费名称
            final String consumerName = stringValueResolver.resolveStringValue(holder.getAnnotation().consumerName());
            //从注解中获取订阅名称
            final String subscriptionName = stringValueResolver.resolveStringValue(holder.getAnnotation().subscriptionName());
            //从注解中获取队列topic名称
            final String topicName = stringValueResolver.resolveStringValue(holder.getAnnotation().topic());
            //获取消费者类型--参考官方文档类型说明
            final SubscriptionType subscriptionType = getSubscriptionType(holder);
            //通过pulsarClient构建consumerBuilder
            final ConsumerBuilder<?> consumerBuilder = pulsarClient
                    .newConsumer()
                    .consumerName(consumerName)
                    .subscriptionName(subscriptionName)
                    .topic(buildTopicUrl(topicName))
                    .subscriptionType(subscriptionType)
                    .messageListener((consumer, msg) -> {
                        try {
                            //从绑定关系中获取需执行的方法
                            final Method method = holder.getHandler();
                            method.setAccessible(true);
                            //通过反射执行注解所在的方法，并将监听到的消息作为参数进行传递
                            method.invoke(holder.getBean(), new String(msg.getValue()));
                            //消息执行后手动ack消息
                            consumer.acknowledge(msg);
                        } catch (Exception e) {
                            //消息处理执行异常，进行negativeAcknowledge操作
                            consumer.negativeAcknowledge(msg);
                        }
                    });
            buildDeadLetterPolicy(holder, consumerBuilder);
            return consumerBuilder.subscribe();
        } catch (PulsarClientException e) {
            //应该自定义异常，这里暂时不做处理
            throw new RuntimeException(e);
        }
    }

    private SubscriptionType getSubscriptionType(ConsumerHolder holder) {
        SubscriptionType subscriptionType = Arrays.stream(holder.getAnnotation().subscriptionType())
                .findFirst().orElse(null);
        if (subscriptionType == null) {
            subscriptionType = DEFAULT_SUBSCRIPTION_TYPE;
        }
        return subscriptionType;
    }

    public void buildDeadLetterPolicy(ConsumerHolder holder, ConsumerBuilder<?> consumerBuilder) {
        DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterBuilder =
                DeadLetterPolicy.builder().maxRedeliverCount(-1);
    }


    public List<Consumer> getConsumers() {
        return consumers;
    }


    @Override
    public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
        this.stringValueResolver = stringValueResolver;
    }

    public String buildTopicUrl(String topic) {
        return tdmqProperties.getClusterId() + "/" + tdmqProperties.getEnvironmentId() +
                "/" + topic;
    }
}
