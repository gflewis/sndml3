package servicenow.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class Util {

	public Util() {
	}
	
	static Logger getRequestLogger(Class cls, Table tbl) {
		return getRequestLogger(cls, tbl.getName());
	}
	
	static Logger getResponseLogger(Class cls, Table tbl) {
		return getResponseLogger(cls, tbl.getName());
	}

	static Logger getRequestLogger(Class cls, String tbl) {
		String loggerName = cls.getName() + "." + tbl + ".request";
		return LoggerFactory.getLogger(loggerName);		
	}
	
	static Logger getResponseLogger(Class cls, String tbl) {
		String loggerName = cls.getName() + "." + tbl + ".request";
		return LoggerFactory.getLogger(loggerName);		
	}

}
