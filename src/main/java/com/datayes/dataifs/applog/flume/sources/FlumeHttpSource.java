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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLServerSocket;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSource;
import org.apache.flume.source.http.HTTPSourceConfigurationConstants;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.tools.HTTPServerConstraintUtil;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * @author qihai.su
 *
 */
public class FlumeHttpSource extends AbstractSource implements
		EventDrivenSource, Configurable {
	/*
	 * There are 2 ways of doing this: a. Have a static server instance and use
	 * connectors in each source which binds to the port defined for that
	 * source. b. Each source starts its own server instance, which binds to the
	 * source's port.
	 * 
	 * b is more efficient than a because Jetty does not allow binding a servlet
	 * to a connector. So each request will need to go through each each of the
	 * handlers/servlet till the correct one is found.
	 */

	private static final Logger LOG = LoggerFactory.getLogger(HTTPSource.class);
	private volatile Integer port;
	private volatile Integer httpsPort;
	private volatile Server srv;
	private volatile String host;
	private HTTPSourceHandler handler;
	private SourceCounter sourceCounter;

	// SSL configuration variable
	private volatile String keyStorePath;
	private volatile String keyStorePassword;
	private volatile Boolean sslEnabled;
	private final List<String> excludedProtocols = new LinkedList<String>();

	@Override
	public void configure(Context context) {
		try {
			// SSL related config
			sslEnabled = context.getBoolean(
					HTTPSourceConfigurationConstants.SSL_ENABLED, false);

			port = context
					.getInteger(HTTPSourceConfigurationConstants.CONFIG_PORT);
			if(port == null){
				port = 80;
			}
			httpsPort = context.getInteger("httpsPort");
			if(httpsPort == null){
				httpsPort = 443;
			}
			host = context.getString(
					HTTPSourceConfigurationConstants.CONFIG_BIND,
					HTTPSourceConfigurationConstants.DEFAULT_BIND);

			Preconditions.checkState(host != null && !host.isEmpty(),
					"HTTPSource hostname specified is empty");
			Preconditions.checkNotNull(port,
					"HTTPSource requires a port number to be" + " specified");

			String handlerClassName = context.getString(
					HTTPSourceConfigurationConstants.CONFIG_HANDLER,
					HTTPSourceConfigurationConstants.DEFAULT_HANDLER).trim();

			if (sslEnabled) {
				LOG.debug("SSL configuration enabled");
				keyStorePath = context
						.getString(HTTPSourceConfigurationConstants.SSL_KEYSTORE);
				
				Preconditions.checkArgument(keyStorePath != null
						&& !keyStorePath.isEmpty(),
						"Keystore is required for SSL Conifguration");
				LOG.debug("keyStorePath=" + keyStorePath);
				keyStorePassword = context
						.getString(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD);
				Preconditions.checkArgument(keyStorePassword != null,
						"Keystore password is required for SSL Configuration");
				String excludeProtocolsStr = context
						.getString(HTTPSourceConfigurationConstants.EXCLUDE_PROTOCOLS);
				if (excludeProtocolsStr == null) {
					excludedProtocols.add("SSLv3");
				} else {
					excludedProtocols.addAll(Arrays.asList(excludeProtocolsStr
							.split(" ")));
					if (!excludedProtocols.contains("SSLv3")) {
						excludedProtocols.add("SSLv3");
					}
				}
			}

			@SuppressWarnings("unchecked")
			Class<? extends HTTPSourceHandler> clazz = (Class<? extends HTTPSourceHandler>) Class
					.forName(handlerClassName);
			handler = clazz.getDeclaredConstructor().newInstance();
			// ref: http://docs.codehaus.org/display/JETTY/Embedding+Jetty
			// ref:
			// http://jetty.codehaus.org/jetty/jetty-6/apidocs/org/mortbay/jetty/servlet/Context.html
			Map<String, String> subProps = context
					.getSubProperties(HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX);
			handler.configure(new Context(subProps));
		} catch (ClassNotFoundException ex) {
			LOG.error("Error while configuring HTTPSource. Exception follows.",
					ex);
			Throwables.propagate(ex);
		} catch (ClassCastException ex) {
			LOG.error("Deserializer is not an instance of HTTPSourceHandler."
					+ "Deserializer must implement HTTPSourceHandler.");
			Throwables.propagate(ex);
		} catch (Exception ex) {
			LOG.error("Error configuring HTTPSource!", ex);
			Throwables.propagate(ex);
		}
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	private void checkHostAndPort() {
		Preconditions.checkState(host != null && !host.isEmpty(),
				"HTTPSource hostname specified is empty");
		Preconditions.checkNotNull(port,
				"HTTPSource requires a port number to be" + " specified");
	}

	@Override
	public void start() {
		Preconditions.checkState(srv == null,
				"Running HTTP Server found in source: " + getName()
						+ " before I started one."
						+ "Will not attempt to start.");
		srv = new Server();

		// Connector Array
//		Connector[] connectors = new Connector[1];
//
//		if (sslEnabled) {
//			SslSocketConnector sslSocketConnector = new HTTPSourceSocketConnector(excludedProtocols);
//			sslSocketConnector.setKeystore(keyStorePath);
//			sslSocketConnector.setPassword(keyStorePassword);
//			sslSocketConnector.setKeyPassword(keyStorePassword);
////			sslSocketConnector.setKeystoreType("PKCS12");
//			sslSocketConnector.setReuseAddress(true);
//			connectors[0] = sslSocketConnector;
//		} else {
//			SelectChannelConnector connector = new SelectChannelConnector();
//			connector.setReuseAddress(true);
//			connectors[0] = connector;
//		}

//		connectors[0].setHost(host);
//		connectors[0].setPort(port);
//		srv.setConnectors(connectors);
		
		List<Connector> connectorList = new ArrayList<>();

		//http
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setReuseAddress(true);
		connector.setHost(host);
		connector.setPort(port);
		connectorList.add(connector);

		if(sslEnabled) {
			//https
			SslSocketConnector sslSocketConnector = new HTTPSourceSocketConnector(excludedProtocols);
			sslSocketConnector.setKeystore(keyStorePath);
			sslSocketConnector.setPassword(keyStorePassword);
			sslSocketConnector.setKeyPassword(keyStorePassword);
//		sslSocketConnector.setKeystoreType("PKCS12");
			sslSocketConnector.setReuseAddress(true);
			sslSocketConnector.setHost(host);
			sslSocketConnector.setPort(port);
			connectorList.add(sslSocketConnector);
		}
		Connector[] connectors = new Connector[connectorList.size()];
		srv.setConnectors(connectorList.toArray(connectors));
		try {
			org.mortbay.jetty.servlet.Context root = new org.mortbay.jetty.servlet.Context(
					srv, "/", org.mortbay.jetty.servlet.Context.SESSIONS);
			root.addServlet(new ServletHolder(new FlumeHTTPServlet()), "/");
			root.addServlet(new ServletHolder(new HeartbeatServlet()), "/heartbeat");
			HTTPServerConstraintUtil.enforceConstraints(root);
			srv.start();
			Preconditions.checkArgument(srv.getHandler().equals(root));
		} catch (Exception ex) {
			LOG.error("Error while starting HTTPSource. Exception follows.", ex);
			Throwables.propagate(ex);
		}
		Preconditions.checkArgument(srv.isRunning());
		sourceCounter.start();
		super.start();
	}

	@Override
	public void stop() {
		try {
			srv.stop();
			srv.join();
			srv = null;
		} catch (Exception ex) {
			LOG.error("Error while stopping HTTPSource. Exception follows.", ex);
		}
		sourceCounter.stop();
		LOG.info("Http source {} stopped. Metrics: {}", getName(),
				sourceCounter);
	}

	private class HeartbeatServlet extends HttpServlet {

		private static final long serialVersionUID = 2992129244007363811L;

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp)
				throws ServletException, IOException {
			resp.setCharacterEncoding("UTF-8");
			resp.setContentType("text/json;charset=utf-8");
			String callBack = req.getParameter("callback");
			String jsonp = "callback(" + callBack + ")";
			try(PrintWriter pw = resp.getWriter();){
				pw.print(jsonp);
			}
		}

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp)
				throws ServletException, IOException {
			doGet(req, resp);
		}
	}


	private class FlumeHTTPServlet extends HttpServlet {

		private static final long serialVersionUID = 4891924863218790344L;

		@Override
		public void doPost(HttpServletRequest request,
				HttpServletResponse response) throws IOException {
			List<Event> events = Collections.emptyList(); // create empty list
			try {
//				response.setHeader("Access-Control-Allow-Origin", "https://192.168.208.130:8443,https://app.wmcloud.com");
				response.setHeader("Access-Control-Allow-Origin", request.getHeader("origin"));
				response.setHeader("Access-Control-Allow-Credentials", "true");
				response.setHeader("Access-Control-Allow-Headers", "X-Requested-With");
				response.setHeader("Access-Control-Allow-Methods","PUT,POST,GET,DELETE,OPTIONS");
				events = handler.getEvents(request);
			} catch (HTTPBadRequestException ex) {
				LOG.warn("Received bad request from client. ", ex);
				response.sendError(HttpServletResponse.SC_BAD_REQUEST,
						"Bad request from client. " + ex.getMessage());
				return;
			} catch (Exception ex) {
				LOG.warn("Deserializer threw unexpected exception. ", ex);
				response.sendError(
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						"Deserializer threw unexpected exception. "
								+ ex.getMessage());
				return;
			}
			sourceCounter.incrementAppendBatchReceivedCount();
			sourceCounter.addToEventReceivedCount(events.size());
			try {
				getChannelProcessor().processEventBatch(events);
			} catch (ChannelException ex) {
				LOG.warn(
						"Error appending event to channel. "
								+ "Channel might be full. Consider increasing the channel "
								+ "capacity or make sure the sinks perform faster.",
						ex);
				response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
						"Error appending event to channel. Channel might be full."
								+ ex.getMessage());
				return;
			} catch (Exception ex) {
				LOG.warn("Unexpected error appending event to channel. ", ex);
				response.sendError(
						HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						"Unexpected error while appending event to channel. "
								+ ex.getMessage());
				return;
			}

			response.setCharacterEncoding(request.getCharacterEncoding());
			response.setStatus(HttpServletResponse.SC_OK);
			if (!CollectionUtils.isEmpty(events)) {
				response.getWriter().append("{\"success\":true}");
			} else {
				response.getWriter().append("{\n\t\"success\":false,\n\t\"message\":\"events is null\"\n}");
			}


			response.flushBuffer();
			
			sourceCounter.incrementAppendBatchAcceptedCount();
			sourceCounter.addToEventAcceptedCount(events.size());
		}

		@Override
		public void doGet(HttpServletRequest request,
				HttpServletResponse response) throws IOException {
			doPost(request, response);
		}
	}

	private static class HTTPSourceSocketConnector extends SslSocketConnector {

		private final List<String> excludedProtocols;

		HTTPSourceSocketConnector(List<String> excludedProtocols) {
			this.excludedProtocols = excludedProtocols;
		}

		@Override
		public ServerSocket newServerSocket(String host, int port, int backlog)
				throws IOException {
			SSLServerSocket socket = (SSLServerSocket) super.newServerSocket(
					host, port, backlog);
			String[] protocols = socket.getEnabledProtocols();
			List<String> newProtocols = new ArrayList<String>(protocols.length);
			for (String protocol : protocols) {
				if (!excludedProtocols.contains(protocol)) {
					newProtocols.add(protocol);
				}
			}
			socket.setEnabledProtocols(newProtocols
					.toArray(new String[newProtocols.size()]));
			return socket;
		}
	}
}
