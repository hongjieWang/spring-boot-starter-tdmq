# 基于腾讯云tdmq消息队列封装SpringBootStarter（二）

关于腾讯云tdmq的基本使用参见《基于腾讯云tdmq消息队列封装SpringBootStarter（一）》，这里我们基于之前的内容在次进行优化封装。

## 一、创建消费者注解（TdmqConsumer）和生产者注解（TdmqProducer）

### 1.1、基础工程回顾

首先我们回顾下上一章完成的基础功能。

![工程目录](https://img-1258527903.cos.ap-beijing.myqcloud.com/img/WX20220103-101905@2x.png)

上一章我们创建了配置目录`config`、生产者和消费者目录，以及META-INF目录和`spring.factories`配置文件。

在此基础上我们继续完善我们的工程。

### 1.2、创建注解

在该工程上新建`annotation`包，并在`annotation`包下创建`TdmqProducer`和`TdmqConsumer`注解。并且在消费者注解`TdmqConsumer`注解中新增一下属性：`topic`、`clazz`、`SubscriptionType`、`consumerName`、`subscriptionName`。

最终消费者注解内容如下：

```java
/**
 * 消费者注解
 *
 * @author wanghongjie
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
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
```

生产者注解：

```java
/**
 * 生产者注解
 *
 * @author wanghongjie
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TdmqProducer {
}
```

### 1.3、创建收集器

我们在工程中创建生产者收集器(`ProducerCollector`)和消费者收集器（`ConsumerCollector`），创建收集器的目的是在springBoot项目启动中，扫描所有带有`TdmqProducer`和`TdmqConsumer`注解的Bean对象，并将其统一管理。

在工程中创建`collector`包，并在该包下创建`ProducerCollector`和`ConsumerCollector`。

在创建消费者收集器前我们需要创建个对象类`ConsumerHolder`和`ProducerCollector`,用来绑定注解和实现类的绑定关系。

#### 1.3.1、创建消费者绑定对象

```java
/**
 * @Author julyWhj
 * @Description $
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
```

#### 1.3.2 生产者绑定对象

```java
/**
 * @Author julyWhj
 * @Description 生产者绑定关系$
 * @Date 2022/1/3 1:26 下午
 **/
public class ProducerHolder {
    private final String topic;
    private final Class<?> clazz;
    private final String serialization;

    public ProducerHolder(String topic, Class<?> clazz, String serialization) {
        this.topic = topic;
        this.clazz = clazz;
        this.serialization = serialization;
    }

    public String getTopic() {
        return topic;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public String getSerialization() {
        return serialization;
    }

}
```



#### 1.3.3、创建消费者收集器`ConsumerCollector`.

```java
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
```

#### 1.3.4、创建生产者收集器

```java
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
```

### 1.4、创建消费者消息处理聚合器

我们通过`postProcessBeforeInitialization`方法以及将全部带有`TdmqConsumer`注解的对象收集起来，接下来我们定义个消费者消息处理器，来出来这些Bean对象，这里也是本篇文章的核心内容。

```java
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
                            method.invoke(holder.getBean(), msg.getValue());
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
```

### 1.5、创建生产者工厂和模版处理类

#### 1.5.1、创建生产者工厂接口`IProducerFactory`

```java
/**
 * @Author julyWhj
 * @Description 生产者工程接口$
 * @Date 2021/12/30 7:55 下午
 **/
public interface IProducerFactory {
    Map<String, ImmutablePair<Class<?>, String>> getTopics();
}
```

#### 1.5.2、创建工程对象

```java
/**
 * @Author julyWhj
 * @Description $
 * @Date 2022/1/3 1:27 下午
 **/
@TdmqProducer
public class ProducerFactory implements IProducerFactory {

    private final Map<String, ImmutablePair<Class<?>, String>> topics = new HashMap<>();

    public ProducerFactory addProducer(String topic) {
        return addProducer(topic, byte[].class, "JSON");
    }

    public ProducerFactory addProducer(String topic, Class<?> clazz) {
        topics.put(topic, new ImmutablePair<>(clazz, "JSON"));
        return this;
    }

    public ProducerFactory addProducer(String topic, Class<?> clazz, String serialization) {
        topics.put(topic, new ImmutablePair<>(clazz, serialization));
        return this;
    }

    @Override
    public Map<String, ImmutablePair<Class<?>, String>> getTopics() {
        return topics;
    }
}
```

#### 1.5.3、构建TdmqTemplate

```java
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
```

### 1.6、整合生产消息者配置

将生产者和消费者配置到`TdmqAutoConfiguration`文件中，完整的`TdmqAutoConfiguration`内容如下：

```java
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
```

## 二、使用案例

我们这里使用自定义的`TdmqConsumer`和`TdmqTemplate`来完成一个生产消费者的案例。

2.1、创建生产者配置类

创建生产者配置类`ProducerConfiguration`,该配置类，主要将消息队列队列名称绑定到ProducerFactory上下文中，我们可以通过`TdmqTemplate`去直接使用。

```java
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
                .addProducer(MESSAGE_LOGGING_TOPIC, String.class);
    }
}
```

创建消费者监听

```java
/**
 * @Author julyWhj
 * @Description 消息队列消费者$
 * @Date 2022/1/3 2:01 下午
 **/
@Slf4j
@Service
public class MessageLoggingListener {
    public static final String MESSAGE_LOGGING_TOPIC = "message_logging";

    @TdmqConsumer(topic = MESSAGE_LOGGING_TOPIC, consumerName = "message_logging", clazz = String.class, subscriptionName = "message_logging_es")
    void consume(String msg) {
        log.info("------------{}", msg);
    }
}
```

去除之前消费者`TdmqConsumer.class`.

修改单元测试`SpringBootStarterTdmqApplicationTests`

```java
@Slf4j
@SpringBootTest
class SpringBootStarterTdmqApplicationTests {
    @Autowired
    private TdmqTemplate proucer;

    @Test
    public void producer() throws PulsarClientException {
        MessageId messageId = proucer.send("message_logging", "发送消息测试");
        log.info("send msg is success Id = {}", messageId);
    }

}
```

将之前的TdmqProucer改为`TdmqTemplate`;

启动单元测试：

查看测试结果：

![测试结果](https://img-1258527903.cos.ap-beijing.myqcloud.com/img/WX20220103-141856@2x.png)

## 三、说明：

### 1、配置生产者工厂

```java
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
                .addProducer(MESSAGE_LOGGING_TOPIC, String.class);
    }
}
```

### 2、创建消费者实现类

```java
/**
 * @Author julyWhj
 * @Description 消息队列消费者$
 * @Date 2022/1/3 2:01 下午
 **/
@Slf4j
@Service
public class MessageLoggingListener {
    public static final String MESSAGE_LOGGING_TOPIC = "message_logging";

    @TdmqConsumer(topic = MESSAGE_LOGGING_TOPIC, consumerName = "message_logging", clazz = String.class, subscriptionName = "message_logging_es")
    void consume(String msg) {
        log.info("------------{}", msg);
    }
}
```

主要在方法上增加`TdmqConsumer`注解。

### 3、`生产者TdmqTemplate`模版使用

```java
@Slf4j
@SpringBootTest
class SpringBootStarterTdmqApplicationTests {
    @Autowired
    private TdmqTemplate proucer;

    @Test
    public void producer() throws PulsarClientException {
        MessageId messageId = proucer.send("message_logging", "发送消息测试");
        log.info("send msg is success Id = {}", messageId);
    }

}
```

### 4、使用配置文件

```yaml
tdmq:
  enable: true
  serviceUrl: serviceUrl
  token: token
  clusterId: clusterId
  environmentId: environmentId
```

## 源码地址：

[hongjieWang/spring-boot-starter-tdmq: spring-boot-starter-tdmq (github.com)](https://github.com/hongjieWang/spring-boot-starter-tdmq)

