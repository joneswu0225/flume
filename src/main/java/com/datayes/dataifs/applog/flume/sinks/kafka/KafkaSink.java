/** 
 * 通联数据机密
 * --------------------------------------------------------------------
 * 通联数据股份公司版权所有 © 2013-1016
 * 
 * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
 * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
 * 版权法保护。
 * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
 * 
 * DataYes CONFIDENTIAL
 * --------------------------------------------------------------------
 * Copyright © 2013-2016 DataYes, All Rights Reserved.
 * 
 * NOTICE: All information contained herein is the property of DataYes 
 * Incorporated. The intellectual and technical concepts contained herein are 
 * proprietary to DataYes Incorporated, and may be covered by China, U.S. and 
 * Other Countries Patents, patents in process, and are protected by trade 
 * secret or copyright law. 
 * Dissemination of this information or reproduction of this material is 
 * strictly forbidden unless prior written permission is obtained from DataYes.
 */
package com.datayes.dataifs.applog.flume.sinks.kafka;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;

/**
 * kafka sink.
 */
public class KafkaSink extends AbstractSink implements Configurable {
    // - [ constant fields ] ----------------------------------------

    /**
     * The constant logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
    
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
    public synchronized void start() {
        //初始化producer
    	super.start();
        ProducerConfig config = new ProducerConfig(this.parameters);
        this.producer = new Producer<String, String>(config);
    }

    @Override
    public synchronized void stop() {
        //关闭producer
    	producer.close();
    }
    
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
        	tx.begin();
        	List<Event> events = getBatchEvens(channel);
        	if(events.size() > 0){
        		LOGGER.debug("get events size:" + events.size());
        		List<KeyedMessage<String, String>> outputDatas = new ArrayList<>();
        		for(Event event : events){
        			KeyedMessage<String, String> temp = toKeyedMessage(event);
        			if(temp != null){
        				outputDatas.add(temp);
        			}
        		}
        		
        		// 将批量的数据通过producer发送给s
 				producer.send(outputDatas);
        	}
        	tx.commit();
        	status = Status.READY;
        } catch (Throwable t) {
        	LOGGER.error("", t);
            status = Status.BACKOFF;
            tx.rollback();
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally{
        	 tx.close();
        }
        return status;
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
					extractOne.put(entry.getKey(), entry.getValue());;
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
			String topic = null;
			if("PRD".equalsIgnoreCase(appEnv) || "product".equalsIgnoreCase(appEnv)){
				topic = topicPrefix + "-" + headers.get("appId");
			}else{
				topic = topicPrefix + "-" + headers.get("appId") + "-" + appEnv;
			}
		    // if partition key does'nt exist
		    if (StringUtils.isEmpty(partitionKey)) {
		    	data = new KeyedMessage<String, String>(topic, extractOne.toJSONString());
		    } else {
		        data = new KeyedMessage<String, String>(topic, partitionKey, extractOne.toJSONString());
		    }
		    
		    return data;
		}catch(Exception e){
			LOGGER.warn("事件扁平化异常, ", e);
			return null;
		}
	}

	/**
	 * 从channel里面获取最多batchSize数量的事件.
	 * @param channel
	 * @return
	 */
	private List<Event> getBatchEvens(Channel channel) {
		List<Event> ret = new ArrayList<Event>();
		Event next = channel.take();
		int count = 0;
		while(next != null && count < batchSize){
			ret.add(next);
			next = channel.take();
			count++;
		}
		return ret;
	}
    
    /*@Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        long batchSize = parameters.get("batchSize") == null?DEFAULT_BATCH_SIZE:Long.parseLong((String)parameters.get("batchSize"));
        LOGGER.debug("batchSize=" + batchSize);
        
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
	
	                LOGGER.debug("get event:" + EventHelper.dumpEvent(event));
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
        	LOGGER.error("", t);
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

