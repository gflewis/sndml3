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
	
	static public synchronized void setGlobalContext() {
		clearContext();
		setContext("table", "_global_");
	}
	
	static public synchronized void setTableContext(Table table) {
		clearContext();
		setTableContext(table.getName());
	}
	
	static public synchronized void setSessionContext(Session session) {
		setContext("user", session.getUsername());
	}

	static public synchronized void setTableContext(String tablename) {
		setContext("table", tablename);
	}
	
	static public synchronized void setWriterContext(Writer writer) {
		setContext("writer", writer.getName());
	}
	
	static public synchronized void setPartitionContext(String partname) {
		setContext("partition", partname);
	}
	
	static public synchronized void setMethodContext(String method) {
		setContext("method", method);
	}
	
	static public synchronized void setURIContext(URI uri) {
		setContext("uri", uri.toString());
	}
	
	static private synchronized void setContext(String name, String value) {
		org.slf4j.MDC.put(name, value);		
	}
	
	static public synchronized void clearContext() {
		org.slf4j.MDC.clear();
	}
	
	@Deprecated
	static private String getContext(String name) {
		return org.slf4j.MDC.get(name);
	}
	
	@Deprecated
	static private boolean hasContext(String name) {
		String context = getContext(name);
		return context != null && context.length() > 0;
	}
	
	static class MessageBuilder {
		StringBuilder msg = new StringBuilder();
		String lastSeparator = "";
		public MessageBuilder(String text) {
			if (text != null) msg.append(text);
		}
		public void addContext(String name, String separator) {
			if (hasContext(name)) {
				msg.append(lastSeparator);
				msg.append(name + "=" + getContext(name));
				lastSeparator = separator;
			}
		}
		public String toString() { 
			return msg.toString(); 
		}
	}
	
	@Deprecated
	static public String getContextMessage() {
		return getContextMessage("");
	}
	
	@Deprecated
	static public String getContextMessage(String message) {
		MessageBuilder msg = new MessageBuilder(message);
		msg.addContext("table",  " ");
		msg.addContext("method",  " ");
		return msg.toString();
	}
}
