package com.datayes.dataifs.applog.flume.sinks.kafka;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;

/**
 * kafka sink.
 */
@Slf4j
public class KafkaSink extends AbstractSink implements Configurable {
    // - [ constant fields ] ----------------------------------------

    /**
     * The Parameters.
     */
    private Properties parameters;
    
    /**
     * The Producer.
     */
    private Producer<String, String> producer;
    
    private String topicPrefix;
    
    private String partitionKey;
    
    private String encoding;
    
    private long batchSize;
    
    private Set<String> excludeAppIds = new HashSet<>();
    
    /**
     * The Context.
     */
    private Context context;
    
    private static final long DEFAULT_BATCH_SIZE = 500;
    
    public void configure(Context context) {
        //读取配置，并检查配置
    	this.context = context;
        ImmutableMap<String, String> props = context.getParameters();

        parameters = new Properties();
        for (String key : props.keySet()) {
            String value = props.get(key);
            this.parameters.put(key, value);
        }
        
        topicPrefix = StringUtils.defaultIfEmpty(
        		(String)this.parameters.get(KafkaFlumeConstans.CUSTOME_TOPIC_PREFIX_KEY),
        		KafkaFlumeConstans.DEFAULT_TOPIC_PREFIX_KEY);
        partitionKey = (String) parameters.get(KafkaFlumeConstans.PARTITION_KEY_NAME);
        encoding = StringUtils.defaultIfEmpty(
                (String) this.parameters.get(KafkaFlumeConstans.ENCODING_KEY_NAME),
                KafkaFlumeConstans.DEFAULT_ENCODING);
        batchSize = parameters.get("batchSize") == null?DEFAULT_BATCH_SIZE:Long.parseLong((String)parameters.get("batchSize"));
        
        String excludeAppIdList = (String)parameters.get(KafkaFlumeConstans.EXCLUDE_APPIDS);
        if(!StringUtils.isEmpty(excludeAppIdList)){
        	String[] splits = excludeAppIdList.split(",");
        	for(String excludeAppId : splits){
        		excludeAppIds.add(excludeAppId);
        	}
        }
    }

    @Override
    public void start() {
        //初始化producer
    	super.start();
        ProducerConfig config = new ProducerConfig(this.parameters);
        this.producer = new Producer<String, String>(config);
    }

    @Override
    public void stop() {
        //关闭producer
    	producer.close();
    }
    
    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
		Event event;
		try {
        	tx.begin();
			List<KeyedMessage<String, String>> resultList = new ArrayList<>();
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();
				if (event != null) {
					KeyedMessage<String, String> temp = toKeyedMessage(event);
					if(temp != null){
						resultList.add(temp);
					}
				} else {
					result = Status.BACKOFF;
					break;
				}
			}
			if(resultList.size() > 0) {
				producer.send(resultList);
				log.info("finish send events size:" + resultList.size());
			}
        	tx.commit();
        } catch (Throwable t) {
			log.info("error in handle kafka process");
        	log.error("", t);
			result = Status.BACKOFF;
            tx.rollback();
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally{
        	 tx.close();
        }
        return result;
    }

	/**
	 * 将事件扁平化. 并封装为keyedMessage
	 * @param event
	 * @throws UnsupportedEncodingException
	 */
	private KeyedMessage<String, String> toKeyedMessage(Event event) throws UnsupportedEncodingException {
		String eventData = new String(event.getBody(), encoding);
		Map<String, String> headers = event.getHeaders();

		String appId = headers.get("appId");
		if(appId == null || excludeAppIds.contains(appId)){
			return null;
		}
		
		//将事件扁平化.
		try{
			JSONObject o = JSON.parseObject(eventData);
			JSONObject extractOne = new JSONObject();
			JSONObject commmonObject = o.getJSONObject("common");
			JSONObject eventObject = o.getJSONObject("event");
			if(commmonObject != null){
				for(Map.Entry<String, Object> entry : commmonObject.entrySet()){
					extractOne.put(entry.getKey(), entry.getValue());
				}
			}
			String appEnv = commmonObject.getString("appEnv");
			if(StringUtils.isEmpty(appEnv)){
			   appEnv = "PRD";
			}

			if(eventObject != null){
				for(Map.Entry<String, Object> entry : eventObject.entrySet()){
					String key = entry.getKey();
					if(!extractOne.containsKey(key)){
						extractOne.put(key, entry.getValue());
					}
				}
			}
			KeyedMessage<String, String> data;
//			String topic = null;
//			if("PRD".equalsIgnoreCase(appEnv) || "product".equalsIgnoreCase(appEnv)){
//				topic = topicPrefix + "-" + headers.get("appId");
//			}else{
//				topic = topicPrefix + "-" + headers.get("appId") + "-" + appEnv;
//			}
			String topic = topicPrefix + "-" + headers.get("appId");
			// if partition key does'nt exist
		    if (StringUtils.isEmpty(partitionKey)) {
		    	data = new KeyedMessage<String, String>(topic, extractOne.toJSONString());
		    } else {
		        data = new KeyedMessage<String, String>(topic, partitionKey, extractOne.toJSONString());
		    }
		    
		    return data;
		}catch(Exception e){
			log.error("事件扁平化异常, " + e.getMessage(), e);
			return null;
		}
	}

	/*
     * 将时间戳转换为时间
     */
	private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String stampToDate(String s){
		return DATETIME_FORMAT.format(new Date(Long.parseLong(s)));
	}
    /*@Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        long batchSize = parameters.get("batchSize") == null?DEFAULT_BATCH_SIZE:Long.parseLong((String)parameters.get("batchSize"));
        log.debug("batchSize=" + batchSize);
        
        try {
                tx.begin();
                
                //将日志按category分队列存放
                List<KeyedMessage<String, String>> eventList = new ArrayList<KeyedMessage<String, String>>();
                
                //从channel中取batchSize大小的日志，从header中获取category，生成topic，并存放于上述的Map中；
                String topicPrefix = StringUtils.defaultIfEmpty(
                		(String)this.parameters.get(KafkaFlumeConstans.CUSTOME_TOPIC_PREFIX_KEY),
                		KafkaFlumeConstans.DEFAULT_TOPIC_PREFIX_KEY);
                String partitionKey = (String) parameters.get(KafkaFlumeConstans.PARTITION_KEY_NAME);
                String encoding = StringUtils.defaultIfEmpty(
                        (String) this.parameters.get(KafkaFlumeConstans.ENCODING_KEY_NAME),
                        KafkaFlumeConstans.DEFAULT_ENCODING);
                
                int txnEventCount = 0;
                for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
	                Event event = channel.take();
	
	                log.debug("get event:" + EventHelper.dumpEvent(event));
	                String eventData = new String(event.getBody(), encoding);
	
	                Map<String, String> headers = event.getHeaders();
	                String topic = topicPrefix + "-" + headers.get("appId");		

	                KeyedMessage<String, String> data;
	                // if partition key does'nt exist
	                if (StringUtils.isEmpty(partitionKey)) {
	                    data = new KeyedMessage<String, String>(topic, eventData);
	                } else {
	                    data = new KeyedMessage<String, String>(topic, partitionKey, eventData);
	                }
	                
	                eventList.add(data);
                }

                //将Map中的数据通过producer发送给kafka
                producer.send(eventList);

               tx.commit();
               status = Status.READY;
        } catch (Throwable t) {
        	log.error("", t);
            tx.rollback();
            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            tx.close();
        }
        return status;
    }*/
}

