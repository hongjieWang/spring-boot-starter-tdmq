package com.tdmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @Author julyWhj
 * @Description tdmq配置文件$
 * @Date 2022/1/2 9:53 上午
 **/
@Data
@ConfigurationProperties(prefix = "tdmq")
public class TdmqProperties {
    /**
     * 启用
     */
    private boolean enable = true;
    /**
     * 服务地址
     */
    private String serviceUrl;
    /**
     * token
     */
    private String token;
    /**
     * 集群ID
     */
    private String clusterId;
    /**
     * 命名空间ID
     */
    private String environmentId;

}
