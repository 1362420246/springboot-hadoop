package com.qbk.hbase;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Threads;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HBase配置
 */
@Configuration
public class HBaseConfig{

    @Autowired
    private HBaseProperties properties;

    /**
     *  配置
     */
    @Bean
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }
        return configuration;
    }

    /**
     *  连接
     * @param configuration 配置
     */
    @Bean
    public Connection connection (org.apache.hadoop.conf.Configuration configuration)throws IOException {
        /**
         * Connection 接口 ConnectionImplementation实现类
         *1.可以直接创建，使用默认连接池：
         *   Connection connection = ConnectionFactory.createConnection(configuration);
         *   默认连接池连接数:
         *   int threads = conf.getInt("hbase.hconnection.threads.max", 256);
         *   默认非核心线程闲置时的超时时长:
         *   long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
         *   默认队列长度:
         *   String HBASE_CLIENT_MAX_TOTAL_TASKS = "hbase.client.max.total.tasks"
         *   int DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS = 100;
         *   threads * DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS
         *2.也可以自定义线程池：以下按照源码默认配置连接池
         */
        int threads = 256 ;
        long keepAliveTime = 60 ;
        BlockingQueue<Runnable> workQueue =
                    new LinkedBlockingQueue<>(threads * 100 );
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(
                threads,
                threads,
                keepAliveTime,
                TimeUnit.SECONDS,
                workQueue,
                Threads.newDaemonThreadFactory(toString() + "-shared"));
        /**
         * 创建连接
         * Connection对象从ConnectionFactory中获取
         * 从Connection实例中获得Table，Admin以及RegionLocator在AS-需要基础。
         * 进程结束，关闭获取的Connection实例。确保Connection在退出之前清理实例。
         * Connections是重量级对象但是线程安全，因此一个应用程序创建一个并保持实例。(单例)
         * Table，Admin和RegionLocator实例是轻量级的。随时创建，然后在关闭它们后立即释放。
         */
        Connection connection = ConnectionFactory.createConnection(configuration,tpe);
        return connection;
    }

}