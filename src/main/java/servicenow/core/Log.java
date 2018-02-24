package servicenow.core;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;

public class Log {

	static public final org.slf4j.Marker INIT     = org.slf4j.MarkerFactory.getMarker("INIT");
	static public final org.slf4j.Marker SCHEMA   = org.slf4j.MarkerFactory.getMarker("SCHEMA");
	static public final org.slf4j.Marker REQUEST  = org.slf4j.MarkerFactory.getMarker("REQUEST");
	static public final org.slf4j.Marker RESPONSE = org.slf4j.MarkerFactory.getMarker("RESPONSE");
	static public final org.slf4j.Marker PROCESS  = org.slf4j.MarkerFactory.getMarker("PROCESS");
	static public final org.slf4j.Marker FINISH   = org.slf4j.MarkerFactory.getMarker("FINISH");
	static public final org.slf4j.Marker TEST     = org.slf4j.MarkerFactory.getMarker("TEST");
	
	@SuppressWarnings("rawtypes")
	static public org.slf4j.Logger logger(Class cls) {
		return org.slf4j.LoggerFactory.getLogger(cls);
	}
	
	static public org.slf4j.Logger logger(String name) {
		return org.slf4j.LoggerFactory.getLogger(name);
	}
	
	static public synchronized void clearContext() {
		org.slf4j.MDC.clear();
	}
	
	static public synchronized void setGlobalContext() {
		clearContext();
		setContextValue("context", "GLOBAL");
	}

	static public void setSchemaContext(Table table) {
		setContext(table, table.getName() + ".schema");
	}
	
	static public synchronized void setContext(Table table, String context) {
		clearContext();
		setContextValue("table", table.getName());
		setContextValue("context", context);
	}
	
	static public synchronized void setTableContext(Table table) {
		setTableContext(table.getName());
		setSessionContext(table.getSession());
	}
	
	static public synchronized void setSessionContext(Session session) {
		setContextValue("user", session.getUsername());
	}

	static public synchronized void setTableContext(String tablename) {
		setContextValue("table", tablename);
	}
			
	static public synchronized void setMethodContext(String method) {
		setContextValue("method", method);
	}
	
	static public synchronized void setURIContext(URI uri) {
		setContextValue("uri", uri.toString());
	}
	
	static private synchronized void setContextValue(String name, String value) {
		org.slf4j.MDC.put(name, value);		
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
		
}
