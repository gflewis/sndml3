package servicenow.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.DateTime;
import servicenow.api.Log;

/**
 * Class which knows how to retrieve a DateTime from the metrics file
 * and perform date arithmetic.
 *
 */
public class DateTimeFactory {

	final DateTime start;
	File metricsFile = null;
	Properties lastValues = null;
	
	final Pattern datePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?");
	final Pattern namePattern = Pattern.compile("[a-z][a-z0-9_.]*", Pattern.CASE_INSENSITIVE);
	final Pattern exprPattern = Pattern.compile("([a-z][a-z0-9_.]*)?([+-])(\\d+)([a-z])", Pattern.CASE_INSENSITIVE);

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	DateTimeFactory() {
		this.start = DateTime.now();
	}
	
	DateTimeFactory(DateTime start, File metricsFile) {
		this.start = (start==null ? DateTime.now() : start);
		this.metricsFile = metricsFile;
	}
	
	DateTimeFactory(DateTime start, Properties lastValues) {
		this.start = (start==null ? DateTime.now() : start);
		this.lastValues = lastValues;
	}

	DateTime getStart() {
		return this.start;
	}

	private Properties getLastValues() throws ConfigParseException {
		if (this.lastValues == null) {
			this.lastValues = new Properties();
			if (this.metricsFile != null) {
				try {
					lastValues.load(new FileReader(this.metricsFile));
				} catch (IOException e) {
					throw new ConfigParseException(e);
				}							
			}
		}
		return lastValues;
	}
	
	DateTime getDate(Object obj) throws ConfigParseException {
		assert obj != null;
		DateTime result;
		logger.debug(Log.INIT, 
			String.format("getDate %s=%s", obj.getClass().getName(), obj.toString()));
		if (obj instanceof java.util.Date) 
			result = new DateTime((java.util.Date) obj);
		else {
			String expr = obj.toString();
			if (datePattern.matcher(expr).matches())
				result = new DateTime(expr);
			else if (namePattern.matcher(expr).matches())
				result = getName(expr);
			else if (exprPattern.matcher(expr).matches())
				result = getExpr(expr);
			else
				throw new ConfigParseException("Invalid datetime: " + expr);
			logger.debug(Log.INIT, String.format("getDate(%s)=%s", expr, result));
		}
		return result;
	}

	private DateTime getName(String name) throws ConfigParseException {
		assert name != null;
		if (name.equalsIgnoreCase("void")) return null;
		if (name.equalsIgnoreCase("empty")) return null;
		if (name.equalsIgnoreCase("start")) return getStart();
		if (name.equalsIgnoreCase("today")) return getStart().truncate();
		if (name.equalsIgnoreCase("last")) return getLast("start");
		if (name.toLowerCase().startsWith("last.")) {
			name = name.substring(5);
			return getLast(name);
		}
		return getLast(name);
	}

	/**
	 * Get a DateTime value from the metrics (properties) file.
	 */
	DateTime getLast(String propName) throws ConfigParseException {
		assert propName != null;
		if (lastValues == null) {
			String message = String.format("No metrics file; unable to determine last \"%s\"", propName);
			logger.error(Log.INIT, message);
			throw new ConfigParseException(message);
		}
		String propValue = getLastValues().getProperty(propName);
		if (propValue == null) 
			throw new ConfigParseException("Property not found: " + propName);
		return new DateTime(propValue);
	}
	
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
