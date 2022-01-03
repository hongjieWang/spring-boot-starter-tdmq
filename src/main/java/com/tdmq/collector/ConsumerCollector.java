package com.tdmq.collector;

import com.tdmq.annotation.TdmqConsumer;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Author julyWhj
 * @Description 消费者收集器$
 * @Date 2022/1/3 10:34 上午
 **/
@Configuration
public class ConsumerCollector implements BeanPostProcessor {
    /**
     * 维护SpringBoot所有bean对象中包含TdmqConsumer注解的实例对象
     */
    private Map<String, ConsumerHolder> consumers = new ConcurrentHashMap<>();

    /**
     * SpringBoot 启动过程中，Bean实例化后加载postProcessBeforeInitialization方法
     *
     * @param bean     bean对象
     * @param beanName 注解所在的方法名称
     * @return
     */
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        final Class<?> beanClass = bean.getClass();
        // 过滤所有的Bean对象，如果包含TdmqConsumer注解的加入到consumers中
        consumers.putAll(Arrays.stream(beanClass.getDeclaredMethods())
                .filter($ -> $.isAnnotationPresent(TdmqConsumer.class))
                .collect(Collectors.toMap(
                        method -> buildConsumerName(beanClass, method),
                        method -> new ConsumerHolder(method.getAnnotation(TdmqConsumer.class), method, bean,
                                getParameterType(method)))));

        return bean;
    }


    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        return bean;
    }

    public Map<String, ConsumerHolder> getConsumers() {
        return consumers;
    }

    public Optional<ConsumerHolder> getConsumer(String methodDescriptor) {
        return Optional.ofNullable(consumers.get(methodDescriptor));
    }

    public static Class<?> getParameterType(Method method) {
        return method.getParameterTypes()[0];
    }

    /**
     * 构建消费者名称
     *
     * @param clazz  对象
     * @param method 方法
     * @return 消费者名称
     */
    public String buildConsumerName(Class<?> clazz, Method method) {
        return clazz.getName() + method.getName() + Arrays
                .stream(method.getGenericParameterTypes())
                .map(Type::getTypeName)
                .collect(Collectors.joining());
    }
}
