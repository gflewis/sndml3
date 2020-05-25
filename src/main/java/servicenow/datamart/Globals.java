package servicenow.datamart;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.*;

public class Globals {

	private static CommandLine cmdline;
	private static Properties properties = new Properties();
	private static DateTime start = DateTime.now();
	private static LoaderConfig config;
	
	public static Boolean warnOnTruncate = true;
	static Logger logger = LoggerFactory.getLogger(Globals.class);
		
	public static void initialize(String[] args) throws IOException, ParseException {
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required().hasArg().
				desc("Profile file name (required)").build());
		initialize(options, args);
	}
	
	public static void initialize(Options options, String[] args) throws ParseException, IOException {
		DefaultParser parser = new DefaultParser();
		cmdline = parser.parse(options,  args);
		if (!options.hasOption("p"))
			throw new CommandOptionsException("Missing option: --profile");		
		String propfilename = cmdline.getOptionValue("p");
		assert propfilename != null;
		loadPropertyFile(new File(propfilename));		
	}
	
	@Deprecated
	private static void loadPropertyFile(File propfile) throws FileNotFoundException, IOException {
		logger.info(Log.INIT, "loadProperties " + propfile.getAbsolutePath());
		Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		Properties raw = new Properties();
		raw.load(new FileInputStream(propfile));
		for (String name : raw.stringPropertyNames()) {
			String value = raw.getProperty(name);
			// If property is in backticks then evaluate as a command 
			Matcher cmdMatcher = cmdPattern.matcher(value); 
			if (cmdMatcher.matches()) {
				logger.info(Log.INIT, "evaluate " + name);
				String command = cmdMatcher.group(1);
				value = evaluate(command);
				if (value == null || value.length() == 0)
					throw new AssertionError(String.format("Failed to evaluate \"%s\"", command));
				logger.debug(Log.INIT, value);
			}
			properties.setProperty(name, value);			
		}
	}

	static void setLoaderConfig(LoaderConfig value) {
		assert value != null;
		config = value;
		start = config.getStart();
		logger.debug(Log.INIT, String.format("setLoaderConfig start=%s metrics=%s",  
				getStart(), getMetricsFile()));
	}
	
	@Deprecated
	static LoaderConfig getLoaderConfig() {
		assert config != null;
		return config;
	}
	
	/**
	 * Used in JUnit tests.
	 */
	@Deprecated
	public static void setStart(DateTime value) {
		Globals.start = value;
	}
	
	@Deprecated
	public static DateTime getStart() {
		assert start != null;
		return start;
	}

	static File getMetricsFile() {
		String filename = getProperty("metrics");
		if (filename != null) return new File(filename);
		if (config == null) return null;
		return config.metricsFile;
	}

	static String getProperty(String name) {
		assert name != null;
		String propname;
		if (name.indexOf(".") > 0) {
			propname = name;
		}
		else {
			String prefix = (name.matches("templates|dialect")) ? "datamart" : "loader";
			propname = prefix + "." + name;			
		}
		String value = null;
		value = System.getProperty(propname);
		if (value != null) value = properties.getProperty(propname);
		return value;
	}
	
	static boolean getPropertyBoolean(String name, boolean defaultValue) {
		String stringValue = getProperty(name);
		if (stringValue == null) return defaultValue;
		return new Boolean(stringValue);
	}
	
	/*
	@Deprecated
	static String getValue(String name) {
		assert name != null;
		String value = config.getString(name);
		if (value == null) value = getProperty(name);
		return value;
	}

	@Deprecated
	static File getFile(String varname) {
		String path = getValue(varname);
		return (path == null) ? null : new File(path); 
	}
	
	@Deprecated
	public static Boolean getBoolean(String varname) {
		String value = getValue(varname);
		return (value == null) ? null : new Boolean(value);
	}
	
	@Deprecated
	static Integer getInteger(String varname) {
		return getInteger(varname, null);
	}
	
	@Deprecated
	static Integer getInteger(String varname, Integer defaultValue) {
		String value = getValue(varname);
		return (value == null) ? defaultValue : new Integer(value);		
	}
	*/
	
	static Properties getProperties() {
		return properties;
	}

	static CommandLine getCommand() {
		return cmdline;
	}
	
	static boolean hasOptionValue(String name) {
		return cmdline.getOptionValue(name) != null;
	}
	
	static String getOptionValue(String name) {
		return cmdline.getOptionValue(name);
	}
	
	static String[] getArgs() {
		return cmdline.getArgs();
	}
	
	static List<String> getArgList() {
		return cmdline.getArgList();
	}
	
	static String getArg(int index) {
		return getArgList().get(index);
	}

	/*
	@Deprecated
	static Session getSession() {
		return new Session(getProperties());
	}
	*/
	
	/*
	 * Pass a string to Runtime.exec() for evaluation
	 * @param command - Command to be executed
	 * @return Result of command with whitespace trimmed
	 * @throws IOException
	 */
	@Deprecated
	static private String evaluate(String command) throws IOException {
		Process p = Runtime.getRuntime().exec(command);
		String output = IOUtils.toString(p.getInputStream(), "UTF-8").trim();
		return output;
	}


}