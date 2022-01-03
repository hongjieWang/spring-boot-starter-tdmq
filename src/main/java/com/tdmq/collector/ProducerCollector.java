package com.tdmq.collector;

import com.tdmq.annotation.TdmqProducer;
import com.tdmq.config.TdmqProperties;
import com.tdmq.producer.IProducerFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.stereotype.Component;
import org.springframework.util.StringValueResolver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Author julyWhj
 * @Description 生产者收集器$
 * @Date 2022/1/3 1:25 下午
 **/
@Component
public class ProducerCollector implements BeanPostProcessor, EmbeddedValueResolverAware {

    private final PulsarClient pulsarClient;
    private final TdmqProperties tdmqProperties;

    private final Map<String, Producer> producers = new ConcurrentHashMap<>();

    private StringValueResolver stringValueResolver;

    public ProducerCollector(PulsarClient pulsarClient, TdmqProperties tdmqProperties) {
        this.pulsarClient = pulsarClient;
        this.tdmqProperties = tdmqProperties;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        final Class<?> beanClass = bean.getClass();
        if (beanClass.isAnnotationPresent(TdmqProducer.class) && bean instanceof IProducerFactory) {
            producers.putAll(((IProducerFactory) bean).getTopics().entrySet().stream()
                    .map($ -> new ProducerHolder(
                            stringValueResolver.resolveStringValue($.getKey()),
                            $.getValue().left,
                            $.getValue().right))
                    .collect(Collectors.toMap(ProducerHolder::getTopic, this::buildProducer)));
        }

        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        return bean;
    }

    private Producer<?> buildProducer(ProducerHolder holder) {
        try {
            return pulsarClient.newProducer(getSchema(holder))
                    .topic(buildTopicUrl(holder.getTopic()))
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> Schema<?> getSchema(ProducerHolder holder) throws RuntimeException {
        return getGenericSchema(holder.getSerialization(), holder.getClazz());
    }

    public Producer getProducer(String topic) {
        return producers.get(stringValueResolver.resolveStringValue(topic));
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
        this.stringValueResolver = stringValueResolver;
    }

    public String buildTopicUrl(String topic) {
        return tdmqProperties.getClusterId() + "/" + tdmqProperties.getEnvironmentId() +
                "/" + topic;
    }


    private static <T> Schema<?> getGenericSchema(String type, Class<T> clazz) throws RuntimeException {
        switch (type) {
            case "JSON": {
                return Schema.JSON(clazz);
            }
            case "AVRO": {
                return Schema.AVRO(clazz);
            }
            case "STRING": {
                return Schema.STRING;
            }
            default: {
                throw new RuntimeException("Unknown producer schema.");
            }
        }
    }
}
