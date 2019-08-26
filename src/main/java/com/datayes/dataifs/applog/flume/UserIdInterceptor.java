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
