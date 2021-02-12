package sndml.datamart;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import sndml.servicenow.DateTime;
import sndml.servicenow.Log;

/**
 * Class which knows how to retrieve a DateTime from the metrics file
 * and perform date arithmetic.
 *
 */
public class DateTimeFactory {

	private final DateTime start;
	private final DateTime lastStart;
	
	final Pattern datePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?");
	final Pattern namePattern = Pattern.compile("[a-z][a-z0-9_.]*", Pattern.CASE_INSENSITIVE);
	final Pattern exprPattern = Pattern.compile("([a-z][a-z0-9_.]*)?([+-])(\\d+)([a-z])", Pattern.CASE_INSENSITIVE);

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	DateTimeFactory() {
		this.start = DateTime.now();
		this.lastStart = null;
	}
	
	DateTimeFactory(DateTime start) {
		this.start = start;
		this.lastStart = null;
	}
	
	DateTimeFactory(DateTime start, DateTime lastStart) {
		this.start = start;
		this.lastStart = lastStart;
	}
	
	DateTimeFactory(DateTime start, File metricsFile) {
		this.start = (start==null ? DateTime.now() : start);
		this.lastStart = getLastStart(metricsFile);
	}
	
	DateTimeFactory(DateTime start, Properties lastValues) {
		this.start = (start==null ? DateTime.now() : start);
		this.lastStart = getLastStart(lastValues);
	}

	static DateTime getLastStart(File metricsFile) {
		Properties lastValues = new Properties();
		try {
			lastValues.load(new FileInputStream(metricsFile));
		} catch (IOException e) {
			throw new ResourceException(e);
		}
		return new DateTime(lastValues.getProperty("start"));		
	}
	
	static DateTime getLastStart(Properties props) {
		return new DateTime(props.getProperty("start"));				
	}
	
	DateTime getStart() {
		return this.start;
	}
	
	boolean hasLastStart() {
		return this.lastStart != null;
	}
	
	DateTime getLastStart() {
		return this.lastStart;
	}
		
//	DateTime getDate(JsonNode obj) throws ConfigParseException {
//		assert obj != null;
//		return getDate(obj.asText());
//	}
	
	DateTime getDate(String expr) throws ConfigParseException {
		assert expr != null;
		assert expr.length() > 0;
		DateTime result;
		// logger.debug(Log.INIT, String.format("getDate %s", expr));		
		if (datePattern.matcher(expr).matches())
			result = new DateTime(expr);
		else if (namePattern.matcher(expr).matches())
			result = getName(expr);
		else if (exprPattern.matcher(expr).matches())
			result = getExpr(expr);
		else
			throw new ConfigParseException("Invalid datetime: " + expr);
		logger.debug(Log.INIT, String.format("getDate(%s)=%s", expr, result));
		return result;
	}
	
	private DateTime getName(String name) throws ConfigParseException {
		assert name != null;
		assert this.start != null;
		if (name.equalsIgnoreCase("void")) return null;
		if (name.equalsIgnoreCase("empty")) return null;
		if (name.equalsIgnoreCase("start")) return getStart();
		if (name.equalsIgnoreCase("today")) return getStart().truncate();
		if (name.equalsIgnoreCase("last")) {
			if (!hasLastStart())
				throw new ConfigParseException("Metrics file not found. Unable to determine value of LAST.");
			return getLastStart();
		}
		throw new ConfigParseException("Invalid name: " + name);
		/*
		// Functionality removed in 3.4
		if (name.toLowerCase().startsWith("last.")) {
			name = name.substring(5);
			return getLast(name);
		}
		return getLast(name);
		*/
	}

	/*
	 * Get a DateTime value from the metrics (properties) file.
	 *
	private DateTime getLast(String propName) throws ConfigParseException {
		assert propName != null;
		Properties values = getLastValues();
		if (values == null) {
			String message = String.format("No metrics file; unable to determine last \"%s\"", propName);
			logger.error(Log.INIT, message);
			throw new ConfigParseException(message);
		}
		String propValue = values.getProperty(propName);
		if (propValue == null) 
			throw new ConfigParseException("Property not found: " + propName);
		return new DateTime(propValue);
	}
	*/
	
	private DateTime getExpr(String text) throws ConfigParseException {
		Matcher m = exprPattern.matcher(text);
		if (m.matches()) {
			String name = m.group(1);
			String op = m.group(2);
			int sign = (op == "+") ? +1 : -1;
			int num = sign * Integer.parseInt(m.group(3));
			String units = m.group(4);
			if (name == null) name = "start";
			DateTime base = getName(name);
			switch (units.toLowerCase()) {
			case "d": return base.addSeconds(num * DateTime.SEC_PER_DAY);
			case "h": return base.addSeconds(num * DateTime.SEC_PER_HOUR);
			case "m": return base.addSeconds(num * DateTime.SEC_PER_MINUTE);
			default:
				throw new ConfigParseException(String.format("Illegal units \"%s\" in \"%s\"", units, text));
			}						
		}
		else {
			throw new ConfigParseException(String.format("Invalid date expresion: %s", text));
		}
	}
	
}
