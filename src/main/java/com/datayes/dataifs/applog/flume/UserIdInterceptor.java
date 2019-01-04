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
package com.datayes.dataifs.applog.flume;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.datayes.dataifs.applog.flume.utils.AesEcryptUtils;

/**
 * @author qihai.su
 * 用户ID拦截器，用来将APP端发送过来的token解密成userId.
 */
public class UserIdInterceptor implements Interceptor {
	private static final Logger LOG = LoggerFactory.getLogger(UserIdInterceptor.class);
	private static final String TOKEN_KEY="userId";
	
	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#initialize()
	 */
	@Override
	public void initialize() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#intercept(org.apache.flume.Event)
	 */
	@Override
	public Event intercept(Event event) {
		byte[] eventBody = event.getBody();
	    JSONObject logObject = JSONObject.parseObject(new String(eventBody));
	    LOG.debug("intercept event:" + logObject);
	    JSONObject commonO = logObject.getJSONObject("common");
	   
	    String encryptedUserId = commonO == null ? null : commonO.getString(TOKEN_KEY);
	    if(encryptedUserId != null){
	    	try {
	    		logObject.put(TOKEN_KEY, AesEcryptUtils.decrypt(encryptedUserId));
			} catch (Exception e) {
				LOG.error("", e);
			}
	    }
	    
	    event.setBody(logObject.toJSONString().getBytes());
	    return event;
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.interceptor.Interceptor#intercept(java.util.List)
	 */
	@Override
	public List<Event> intercept(List<Event> arg0) {
		for(Event event : arg0){
			intercept(event);
		}
		return arg0;
	}
	
	public static class Builder implements Interceptor.Builder{
	     @Override
	     public void configure(Context context) {
	        // TODO Auto-generated method stub
	     }

	     @Override
	     public Interceptor build() {
	        return new UserIdInterceptor();
	     }
	}
}
