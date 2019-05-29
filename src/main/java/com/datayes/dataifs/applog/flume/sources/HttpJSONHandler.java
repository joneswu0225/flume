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
package com.datayes.dataifs.applog.flume.sources;

import java.io.BufferedReader;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datayes.dataifs.applog.flume.utils.AesEcryptUtils;
import com.google.gson.JsonSyntaxException;

/**
 * @author qihai.su
 *
 */
@Slf4j
public class HttpJSONHandler implements HTTPSourceHandler {

	private static String base64Model = "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$";


	/* (non-Javadoc)
	 * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
	 */
	@Override
	public void configure(Context context) {
		// TODO Auto-generated method stub
	}

	/* (non-Javadoc)
	 * @see org.apache.flume.source.http.HTTPSourceHandler#getEvents(javax.servlet.http.HttpServletRequest)
	 */
	@Override
	public List<Event> getEvents(HttpServletRequest request)
			throws HTTPBadRequestException, Exception {
		String charset = request.getCharacterEncoding();
		//UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
		//be assumed.
		if (charset == null) {
			log.debug("Charset is null, default charset of UTF-8 will be used.");
			charset = "UTF-8";
		} else if (!(charset.equalsIgnoreCase("utf-8")
				|| charset.equalsIgnoreCase("utf-16")
				|| charset.equalsIgnoreCase("utf-32"))) {
			log.error("Unsupported character set in request {}. "
					+ "JSON handler supports UTF-8, "
					+ "UTF-16 and UTF-32 only.", charset);
			throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
					+ "UTF-16 and UTF-32 only.");
		}

	    /*
	     * Gson throws Exception if the data is not parseable to JSON.
	     * Need not catch it since the source will catch it and return error.
	     */
		List<Event> eventList = new ArrayList<Event>(0);
		try (BufferedReader reader = request.getReader();) {
			Cookie[] cookies = request.getCookies();
			Map<String, String> cookieMap = new HashMap<>();
			if(cookies != null){
				for(Cookie cookie : cookies){
					try {
						cookieMap.put(cookie.getName().trim(), URLDecoder.decode(cookie.getValue(), "utf-8"));
					}catch (Exception e){
						cookieMap.put(cookie.getName().trim(), cookie.getValue());
					}
				}
				log.debug("cookie from " + request.getRequestURI() + " " + JSON.toJSONString(cookies));
			}
			String temp=null;
			StringBuilder sb = new StringBuilder();
			while((temp = reader.readLine()) != null){
				sb.append(temp);
			}
			String ip = getIp(request);
			String result = sb.toString();
			log.debug("get request[ " + ip + " ]:" + result);
			JSONArray array = JSON.parseArray(result);
			if(array != null){
				Long appId = array.getJSONObject(0).getJSONObject("common").getLong("appId");
				log.info("get request[ " + ip + " ] appId :" + appId);
				for(int i=0; i<array.size(); i++){
					JSONObject requestO = array.getJSONObject(i);
					JSONObject commonO = requestO.getJSONObject("common");
					String userId = commonO.getString("userId");
					appId = commonO.getLong("appId");
					//进行userId解密
					commonO.put("ip", ip);
					if(userId != null && Pattern.matches(base64Model, userId)){
						commonO.put("userId", AesEcryptUtils.decrypt(userId));
					}
					if(cookieMap.size() > 0){
						commonO.put("cookie", cookieMap);
					}
					JSONArray events = requestO.getJSONArray("events");
					String appEnv = commonO.getString("appEnv");
					if(StringUtils.isEmpty( appEnv)){
						appEnv = "PRD";
					}
					commonO.put("appEnv", appEnv.toUpperCase());
					if(events != null){
						//current page url.
						String referer=request.getHeader("Referer");

						for(int j=0; j < events.size(); j++){
							JSONObject event = events.getJSONObject(j);
							String eventId = event.getString("eventId");
							Long timestamp = event.getLong("timestamp");
							for(String key: event.keySet()){
								if(event.getString(key) != null) {
									try {
										event.put(key, URLDecoder.decode(event.getString(key), "UTF-8"));
									}catch (Exception e){
										event.put(key, event.getString(key));
									}
								}
							}
							if(referer != null){
								event.put("referer", referer);
							}
							event.put("recordTimestamp", timestamp);
							event.put("recordTime", stampToDate(timestamp));
							long curTimestamp = System.currentTimeMillis();
							event.put("timestamp", curTimestamp);
							event.put("appeartime", stampToDate(curTimestamp));

							JSONEvent e = new JSONEvent();
							Map<String, String> headers = new HashMap<String, String>();
							headers.put("appId", appId.toString());
							headers.put("eventId", eventId);
							headers.put("timestamp", String.valueOf(curTimestamp));
							headers.put("appEnv", appEnv);

							JSONObject newEventBody = new JSONObject();
							newEventBody.put("common", commonO);
							newEventBody.put("event", event);

							e.setHeaders(headers);
							e.setBody(newEventBody.toJSONString().getBytes("utf-8"));
							eventList.add(e);
						}
					}
				}
			}
		} catch (JsonSyntaxException ex) {
			throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
		}

		for (Event e : eventList) {
			((JSONEvent) e).setCharset(charset);
		}
		log.debug("parse request, generate JSONEvent:" + eventList.size());
		return getSimpleEvents(eventList);
	}

	private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String stampToDate(long s){
		return DATETIME_FORMAT.format(new Date(s));
	}

	public static String getIp(HttpServletRequest request) {
		String ip = request.getHeader("X-Forwarded-For");
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("HTTP_CLIENT_IP");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("HTTP_X_FORWARDED_FOR");
		}
		if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}
		return ip;
	}

	private List<Event> getSimpleEvents(List<Event> events) {
		List<Event> newEvents = new ArrayList<Event>(events.size());
		for(Event e:events) {
			newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
		}
		return newEvents;
	}
}
