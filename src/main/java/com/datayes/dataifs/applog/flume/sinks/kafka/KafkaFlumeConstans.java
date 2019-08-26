package com.datayes.dataifs.applog.flume.sinks.kafka;

/**
 * Flume Kafka Constans.
 * User: beyondj2ee
 * Date: 13. 9. 9
 * Time: AM 11:05
 */
public class KafkaFlumeConstans {

    /**
     * The constant PARTITION_KEY_NAME.
     */
    public static final String PARTITION_KEY_NAME = "custom.partition.key";
    /**
     * The constant ENCODING_KEY_NAME.
     */
    public static final String ENCODING_KEY_NAME = "custom.encoding";
    /**
     * The constant DEFAULT_ENCODING.
     */
    public static final String DEFAULT_ENCODING = "UTF-8";
    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_TOPIC_KEY_NAME = "custom.topic.name";
    
    /**
     * 配置的topic前缀
     */
    public static final String CUSTOME_TOPIC_PREFIX_KEY = "custom.topic.prefix.key";
    
    /**
     * 默认的topic前缀
     */
    public static final String DEFAULT_TOPIC_PREFIX_KEY = "applog";

    /**
     * 推送到kafka集群, 排除掉的应用
     */
    public static final String EXCLUDE_APPIDS = "custom.exclude.appids";   
    
    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME = "custom.thread.per.consumer";

}
