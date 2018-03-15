package servicenow.api;

import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class Log {

	static public final Marker INIT     = MarkerFactory.getMarker("INIT");
	static public final Marker SCHEMA   = MarkerFactory.getMarker("SCHEMA");
	static public final Marker REQUEST  = MarkerFactory.getMarker("REQUEST");
	static public final Marker RESPONSE = MarkerFactory.getMarker("RESPONSE");
	static public final Marker PROCESS  = MarkerFactory.getMarker("PROCESS");
	static public final Marker BIND     = MarkerFactory.getMarker("BIND");
	static public final Marker FINISH   = MarkerFactory.getMarker("FINISH");
	static public final Marker TEST     = MarkerFactory.getMarker("TEST");
	
	static class ContextValues {
		String process;
		String table;
		Session session;
		URI uri;
		String method;		
	}
	
	class ContextStack extends ArrayList<ContextValues> {
		private static final long serialVersionUID = 1L;		
	}
	
	private static ThreadLocal<ContextStack> threadStacks;
		
	public static synchronized void popContext() {
		ContextStack stack = threadStacks.get();
		int top = stack.size() - 1;
		if (top >= 0) {
			stack.remove(top);
		}
	}
	
	private static synchronized void pushContext(ContextValues values) {
		ContextStack stack = threadStacks.get();
		stack.add(values);
		setContextValue("context", values.process);
		setContextValue("table", values.table);
		setContextValue("user", values.session.getUsername());
		setContextValue("uri", values.uri.toString());
		setContextValue("method", values.method);
	}
	
	public static synchronized void pushContext(String process, String table, Session session, URI uri, String method) {
		ContextStack stack = threadStacks.get();
		int top = stack.size() - 1;
		ContextValues last = (top >= 0 ? stack.get(top) : null);
		ContextValues values = new ContextValues();
		values.process = (process==null && last!=null ? last.process : process);
		values.table   = (table==null   && last!=null ? last.table : table);
		values.session = (session==null && last!=null ? last.session : session);
		values.uri     = (uri==null     && last!=null ? last.uri : uri);
		values.method  = (method==null  && last!=null ? last.method : method);
		pushContext(values);
	}
	
	public static void pushContext(String process, String table, Session session) {
		pushContext(process, table, session, null, null);
	}
		
	public static void pushContext(Table table, String process) {
		pushContext(process, table.getName(), table.getSession(), null, null);
	}
	
	void pushGlobalContext() {
		clearContext();
		pushContext("GLOBAL", null, null, null, null);
	}
	
	public static void pushURIContext(URI uri) {
		pushContext(null, null, null, uri, null);
	}
	
	public static void pushMethodContext(Table table, String method) {
		pushContext(null, table.getName(), null, null, method);
	}
	
	static public synchronized void clearContext() {
		org.slf4j.MDC.clear();
	}

	@Deprecated
	static public synchronized void setGlobalContext() {
		clearContext();
		setContextValue("context", "GLOBAL");
	}

	@Deprecated
	static public void setSchemaContext(Table table) {
		setContext(table, table.getName() + ".schema");
	}
	
	@Deprecated
	static public synchronized void setContext(Table table, String context) {
		clearContext();
		setContextValue("table", table.getName());
		setContextValue("context", context);
	}
	
	@Deprecated
	static public synchronized void setMethodContext(Table table, String method) {
		setTableContext(table);
		setContextValue("method", method);
	}
	
	@Deprecated
	static public synchronized void setTableContext(Table table) {
		setTableContext(table.getName());
		setSessionContext(table.getSession());
	}
	
	@Deprecated
	static public synchronized void setSessionContext(Session session) {
		setContextValue("user", session.getUsername());
	}

	@Deprecated
	static public synchronized void setTableContext(String tablename) {
		setContextValue("table", tablename);
	}
			
//	static public synchronized void setURIContext(URI uri) {
//		setContextValue("uri", uri.toString());
//	}
	
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

	@SuppressWarnings("rawtypes")
	static public org.slf4j.Logger logger(Class cls) {
		return org.slf4j.LoggerFactory.getLogger(cls);
	}
	
	static public org.slf4j.Logger logger(String name) {
		return org.slf4j.LoggerFactory.getLogger(name);
	}
	
}
