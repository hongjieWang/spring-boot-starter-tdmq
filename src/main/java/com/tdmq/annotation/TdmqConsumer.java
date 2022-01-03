package com.tdmq.annotation;

import com.sun.xml.internal.ws.developer.Serialization;
import org.apache.pulsar.client.api.SubscriptionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 消费者注解
 *
 * @author wanghongjie
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TdmqConsumer {
    /**
     * 订阅主题
     *
     * @return
     */
    String topic();

    /**
     * 序列化类
     *
     * @return
     */
    Class<?> clazz() default byte[].class;

    /**
     * 消费者类型
     *
     * @return
     */
    SubscriptionType[] subscriptionType() default {};

    /**
     * 消费者名称
     *
     * @return
     */
    String consumerName() default "";

    /**
     * 订阅对象名称
     *
     * @return
     */
    String subscriptionName() default "";

}
