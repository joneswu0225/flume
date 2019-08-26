package com.datayes.dataifs.applog.flume.sources;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.net.URLDecoder;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.*;

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
		String result;
		String ip;
		Map<String, String> cookieMap = new HashMap<>();
		try (BufferedReader reader = request.getReader();) {
			Cookie[] cookies = request.getCookies();
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
			ip = getIp(request);
			result = sb.toString();
			log.debug("get request[ " + ip + " ]:" + result);
		} catch (Exception ex) {
			throw new HTTPBadRequestException("Fail to get request content");
		}
		try {
			JSONArray array = JSON.parseArray(result);
			if (array != null) {
				Long appId = array.getJSONObject(0).getJSONObject("common").getLong("appId");
				log.info("get request[ " + ip + " ] appId :" + appId);
				Map<String, String> headerMap = new HashMap<>();
				Enumeration<String> headerNames = request.getHeaderNames();
				while (headerNames.hasMoreElements()){
					String name = headerNames.nextElement();
					headerMap.put(name.replaceAll("-",""), request.getHeader(name));
				}
				if(!headerMap.containsKey("referer")){
					headerMap.put("referer", "");
				}
				for (int i = 0; i < array.size(); i++) {
					JSONObject requestO = array.getJSONObject(i);
					JSONObject commonO = requestO.getJSONObject("common");
					String userId = commonO.getString("userId");
					appId = commonO.getLong("appId");
					//进行userId解密
					commonO.put("ip", ip);
//					if (userId != null && Pattern.matches(base64Model, userId)) {
//						commonO.put("userId", AesEcryptUtils.decrypt(userId));
//					}
					if (cookieMap.size() > 0) {
						commonO.put("cookie", cookieMap);
					}
					if(headerMap.size() > 0){
						commonO.put("header", headerMap);
					}
					JSONArray events = requestO.getJSONArray("events");
					String appEnv = commonO.getString("appEnv");
					if (StringUtils.isEmpty(appEnv)) {
						appEnv = "PRD";
					}
					commonO.put("appEnv", appEnv.toUpperCase());
					String referer;
					if(commonO.containsKey("referer")){
						referer = commonO.getString("referer");
						referer = referer.length() > 500 ? referer.substring(0,500) : referer;
					} else {
						referer = request.getHeader("Referer");
					}
					commonO.put("referer", referer);

					if (events != null) {
						for (int j = 0; j < events.size(); j++) {
							JSONObject event = events.getJSONObject(j);
							Long eventId = event.getLong("eventId");
							Long timestamp = event.getLong("timestamp");
							for (String key : event.keySet()) {
								if (event.getString(key) != null) {
									try {
										event.put(key, URLDecoder.decode(event.getString(key), "UTF-8"));
									} catch (Exception e) {
										event.put(key, event.getString(key));
									}
								}
							}
							event.put("eventId", eventId);
							event.put("recordTimestamp", timestamp);
							event.put("recordTime", stampToDate(timestamp));
							long curTimestamp = System.currentTimeMillis();
							event.put("timestamp", curTimestamp);
							event.put("appeartime", stampToDate(curTimestamp));

							JSONEvent e = new JSONEvent();
							Map<String, String> headers = new HashMap<String, String>();
							headers.put("appId", appId.toString());
							headers.put("eventId", eventId.toString());
							headers.put("timestamp", String.valueOf(curTimestamp));
							headers.put("appEnv", appEnv);

							JSONObject newEventBody = new JSONObject();
							newEventBody.put("common", commonO);
							newEventBody.put("event", event);

							e.setHeaders(headers);
							e.setBody(newEventBody.toJSONString().getBytes("utf-8"));
							eventList.add(e);
						}
						log.info(String.format("[extract events] push %s events to channel", eventList.size()));
					}
				}
			}
		} catch (Exception e){
			log.error("Request has invalid JSON Syntax. request body: " + result, e);
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
