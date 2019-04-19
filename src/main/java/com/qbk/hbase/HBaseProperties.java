package com.qbk.hbase;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * HBase配置属性
 */
@Component
@ConfigurationProperties(prefix="hbase")
@Data
public class HBaseProperties {
    private Map<String, String> config;
}

