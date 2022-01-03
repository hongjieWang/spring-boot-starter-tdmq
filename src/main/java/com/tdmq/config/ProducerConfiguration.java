package com.tdmq.config;

import com.tdmq.producer.ProducerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author julyWhj
 * @Description $
 * @Date 2022/1/3 1:58 下午
 **/
@Configuration
public class ProducerConfiguration {
    /**
     * 队列名称
     */
    public static final String MESSAGE_LOGGING_TOPIC = "message_logging";

    @Bean
    public ProducerFactory producerFactory() {
        //将队列添加到ProducerFactory上下文中
        return new ProducerFactory()
                .addProducer(MESSAGE_LOGGING_TOPIC, Object.class);
    }
}
