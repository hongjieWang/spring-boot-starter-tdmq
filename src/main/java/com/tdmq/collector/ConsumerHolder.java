package com.tdmq.collector;

import com.tdmq.annotation.TdmqConsumer;

import java.lang.reflect.Method;

/**
 * @Author julyWhj
 * @Description $
 * @Date 2021/12/30 11:38 上午
 **/
public class ConsumerHolder {
    private final TdmqConsumer annotation;
    private final Method handler;
    private final Object bean;
    private final Class<?> type;

    ConsumerHolder(TdmqConsumer annotation, Method handler, Object bean, Class<?> type) {
        this.annotation = annotation;
        this.handler = handler;
        this.bean = bean;
        this.type = type;
    }

    public TdmqConsumer getAnnotation() {
        return annotation;
    }

    public Method getHandler() {
        return handler;
    }

    public Object getBean() {
        return bean;
    }

    public Class<?> getType() {
        return type;
    }

    public boolean isWrapped() {
        return type.isAssignableFrom(Object.class);
    }
}
