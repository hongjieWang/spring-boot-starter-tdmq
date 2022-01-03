package com.tdmq.config;

import com.tdmq.collector.ConsumerCollector;
import com.tdmq.collector.ProducerCollector;
import com.tdmq.consumer.ConsumerAggregator;
import com.tdmq.producer.TdmqTemplate;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * @Author julyWhj
 * @Description Mq自动装配类$
 * @Date 2022/1/2 9:59 上午
 **/
@Slf4j
@Data
@EnableConfigurationProperties({TdmqProperties.class})
public class TdmqAutoConfiguration {
    /**
     * Pulsar 客户端
     * 推荐一个进程一个实例
     *
     * @return {@link TdmqAutoConfiguration}
     */
    @Bean
    @ConditionalOnMissingBean(PulsarClient.class)
    @ConditionalOnProperty(name = "tdmq.enable", havingValue = "true")
    public PulsarClient pulsarClient(TdmqProperties mqProperties) throws PulsarClientException {
        log.info("-----------------");
        return PulsarClient.builder()
                .serviceUrl(mqProperties.getServiceUrl())
                .authentication(AuthenticationFactory.token(mqProperties.getToken()))
                .build();
    }

    /**
     * 配置消费者收集器
     *
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(ConsumerCollector.class)
    @ConditionalOnProperty(name = "tdmq.enable", havingValue = "true")
    public ConsumerCollector consumerCollector() {
        return new ConsumerCollector();
    }

    /**
     * 配置消费者消费者消息处理聚合器
     *
     * @param consumerCollector 配置消费者收集器
     * @param pulsarClient      pulsarClient 客户端
     * @param tdmqProperties    配置信息
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(ConsumerAggregator.class)
    @ConditionalOnProperty(name = "tdmq.enable", havingValue = "true")
    public ConsumerAggregator consumerAggregator(ConsumerCollector consumerCollector, PulsarClient pulsarClient, TdmqProperties tdmqProperties) {
        return new ConsumerAggregator(consumerCollector, pulsarClient, tdmqProperties);
    }

    /**
     * 配置生产者收集器
     *
     * @param pulsarClient   pulsarClient 客户端
     * @param tdmqProperties 配置信息
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(ProducerCollector.class)
    @ConditionalOnProperty(name = "tdmq.enable", havingValue = "true")
    public ProducerCollector producerCollector(PulsarClient pulsarClient,
                                               TdmqProperties tdmqProperties) {
        return new ProducerCollector(pulsarClient, tdmqProperties);
    }

    /**
     * 生产者消息模版
     *
     * @param producerCollector 生产者收集器
     * @return
     */
    @Bean
    @ConditionalOnMissingBean(TdmqTemplate.class)
    @ConditionalOnProperty(name = "tdmq.enable", havingValue = "true")
    public TdmqTemplate pulsarTemplate(ProducerCollector producerCollector) {
        return new TdmqTemplate(producerCollector);
    }

}
