package servicenow.api;

import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class Log {

	static public final Marker INIT     = MarkerFactory.getMarker("INIT");
	static public final Marker SCHEMA   = MarkerFactory.getMarker("SCHEMA");
	static public final Marker REQUEST  = MarkerFactory.getMarker("REQUEST");
	static public final Marker RESPONSE = MarkerFactory.getMarker("RESPONSE");
	static public final Marker PROCESS  = MarkerFactory.getMarker("PROCESS");
	static public final Marker FINISH   = MarkerFactory.getMarker("FINISH");
	static public final Marker TEST     = MarkerFactory.getMarker("TEST");
	
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
	
	static public synchronized void setMethodContext(Table table, String method) {
		setTableContext(table);
		setContextValue("method", method);
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
