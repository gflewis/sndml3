package sndml.util;

import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import sndml.agent.AgentMain;
import sndml.loader.JobConfig;
import sndml.servicenow.Table;

public class Log {

	static public final Marker INIT     = MarkerFactory.getMarker("INIT");
	static public final Marker WSDL     = MarkerFactory.getMarker("WSDL");
	static public final Marker SCHEMA   = MarkerFactory.getMarker("SCHEMA");
	static public final Marker REQUEST  = MarkerFactory.getMarker("REQUEST");
	static public final Marker RESPONSE = MarkerFactory.getMarker("RESPONSE");
	static public final Marker PROCESS  = MarkerFactory.getMarker("PROCESS");
	static public final Marker BIND     = MarkerFactory.getMarker("BIND");
	static public final Marker FINISH   = MarkerFactory.getMarker("FINISH");
	static public final Marker ERROR    = MarkerFactory.getMarker("ERROR");
	static public final Marker TEST     = MarkerFactory.getMarker("TEST");
	
	static boolean isShutdown = false;
	
	@SuppressWarnings("rawtypes")
	static public org.slf4j.Logger getLogger(Class cls) {
		return org.slf4j.LoggerFactory.getLogger(cls);
	}
	
	static public org.slf4j.Logger getLogger(String name) {
		return org.slf4j.LoggerFactory.getLogger(name);
	}
	
	static public void shutdown() {
		if (isShutdown) 
			throw new IllegalStateException("Already shutdown");
		org.apache.logging.log4j.LogManager.shutdown();
		isShutdown = true;
	}
	
	static public void setGlobalContext() {
		setGlobalContext(AgentMain.getAgentName());
	}
	
	static public void setGlobalContext(String agentName) {
		MDC.clear();
		MDC.put("agent", agentName);
		MDC.put("job", "GLOBAL");
	}
	
	static public void setThreadName(String name) {
		Thread.currentThread().setName(name);
	}
	
	static public void setTableContext(Table table, String jobname) {
		MDC.clear();
		MDC.put("agent", AgentMain.getAgentName());
		MDC.put("table", table.getName());
		MDC.put("user", table.getSession().getUsername());
		MDC.put("job", jobname);
	}

	static public void setTableContext(Table table) {
		MDC.put("table", table.getName());
		MDC.put("user", table.getSession().getUsername());
	}
	
	static public void setTableContext(String tablename) {
		MDC.put("table", tablename);
	}
				
	static public void setJobContext(String jobname) {
		MDC.put("job", jobname);
	}
	
	static public void setJobContext(JobConfig config) {
		MDC.put("job", config.getName());
	}
	
	static public String getJobContext() {
		return MDC.get("job");
	}
	
	static public void setMethodContext(Table table, String method) {
		setTableContext(table);
		MDC.put("method", method);
	}
	
	static public void setURIContext(URI uri) {
		MDC.put("uri", uri.toString());
	}
	
	static public void clearURIContext() {
		MDC.put("uri",  "");
	}
	
	public static String join(URI uri, String requestText) {
		StringBuilder result = new StringBuilder(uri.toString());
		if (requestText != null) {
			result.append("\n");
			result.append(requestText);
		}
		return result.toString();
	}
	
	public static String joinLines(String str1, String str2) {
		if (str2 == null || str2.length() == 0) return str1;
		return (str1 + "\n" + abbreviate(str2));
	}
	
	public static String abbreviate(String message) {
		final int default_limit = 2048;
		return StringUtils.abbreviate(message, default_limit);
	}
	
	public static void banner(Logger logger, Marker marker, String msg) {
		int len = msg.length();
		String bar = "\n" + StringUtils.repeat("*", len + 4);
		logger.info(marker, DateTime.now() + " " + msg + bar + "\n* " + msg + " *" + bar);
	}
	

}
