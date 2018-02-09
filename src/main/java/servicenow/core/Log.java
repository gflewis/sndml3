package servicenow.core;

import java.net.URI;

public class Log {

	static public final org.slf4j.Marker INIT     = org.slf4j.MarkerFactory.getMarker("INIT");
	static public final org.slf4j.Marker REQUEST  = org.slf4j.MarkerFactory.getMarker("REQUEST");
	static public final org.slf4j.Marker RESPONSE = org.slf4j.MarkerFactory.getMarker("RESPONSE");
	static public final org.slf4j.Marker PROCESS  = org.slf4j.MarkerFactory.getMarker("PROCESS");
	static public final org.slf4j.Marker TERM     = org.slf4j.MarkerFactory.getMarker("TERM");
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
		setContextValue("context", "_global_");
	}

	static public synchronized void setContext(Table table, Writer writer) {
		clearContext();
		setTableContext(table);
		setWriterContext(writer);
	}
	static public synchronized void setTableContext(Table table) {
		setSessionContext(table.getSession());
		setTableContext(table.getName());
	}
	
	static public synchronized void setSessionContext(Session session) {
		setContextValue("user", session.getUsername());
	}

	static public synchronized void setTableContext(String tablename) {
		setContextValue("table", tablename);
		setContextValue("context", tablename);
	}
	
	static public synchronized void setWriterContext(Writer writer) {
		setWriterContext(writer.getName());
	}
	
	static public synchronized void setWriterContext(String writername) {
		setContextValue("writer", writername);
		setContextValue("context", writername);
	}
	static public synchronized void setPartitionContext(String partname) {
		setContextValue("partition", partname);
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
		
}
