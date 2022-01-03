package com.tdmq;

import com.tdmq.producer.TdmqProucer;
import com.tdmq.producer.TdmqTemplate;
import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONUtil;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.org.apache.avro.data.Json;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

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
