package sndml.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.ListIterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.loader.CommandOptionsException;

public class PropertiesEditor {

	private static final Logger logger = LoggerFactory.getLogger(PropertiesEditor.class);

	/**
	 * Generate a Create Table statement.
	 * 
	 * To print out list of properties
	 * <code>
	 *   --help
	 * </code>
	 * 
	 * To validate properties file
	 * <code>
	 *   --validate filename
	 * </code>
	 * 
	 */	
	public static void main(String[] args) throws Exception {
		Option optValidate = 
			Option.builder("v").longOpt("validate").required(false).hasArg(true).
			desc("Validate property file").build();
		Option optHelp =
			Option.builder("h").longOpt("help").required(false).hasArg(false).
			desc("Print list of available properties").build();
		
		Options options = new Options();
		options.addOption(optValidate);
		options.addOption(optHelp);
		CommandLine cmdline = new DefaultParser().parse(options, args);
		if (cmdline.hasOption(optHelp)) {
			printPropertiesTable();
		}
		else if (cmdline.hasOption(optValidate)) {
			String filename = cmdline.hasOption(optValidate) ? 
					cmdline.getOptionValue(optValidate) : null;
			Properties properties = new Properties();
			properties.load(new FileInputStream(filename));
			properties = replacePropertyNames(properties, true, true);
//			properties.store(System.out, filename);			
		}
		else 
			throw new CommandOptionsException(
				String.format("Must specify --%s or --%s",
					optHelp.getLongOpt(), optValidate.getLongOpt()));			
	}

	/**
	 * Substitute environment variables found in property values
	 */
	public static Properties replaceEnvVariables(Properties props) {
		StringSubstitutor envMap = 
			new org.apache.commons.text.StringSubstitutor(System.getenv());
		for (String name : props.stringPropertyNames()) {
			String value = props.getProperty(name);
			String newValue = envMap.replace(value);
			if (!newValue.equals(value)) props.setProperty(value, newValue);			
		}
		return props;		
	}
	
	/**
	 * If property value is enclosed in backtics then evaluate as a command
	 */
	public static Properties replaceCommands(Properties props) throws IOException {
		final Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		for (String name : props.stringPropertyNames()) {
			String value = props.getProperty(name);
			// If property is in backticks then evaluate as a command 
			Matcher cmdMatcher = cmdPattern.matcher(value); 
			if (cmdMatcher.matches()) {
				logger.info(Log.INIT, "evaluate " + name);
				String command = cmdMatcher.group(1);
				value = evaluate(command);
				if (value == null || value.length() == 0)
					throw new AssertionError(String.format("Failed to evaluate \"%s\"", command));
				logger.debug(Log.INIT, value);
				props.setProperty(name, value);
			}
		}
		return props;		
	}
	
	/**
	 * Pass a string to Runtime.exec() for evaluation
	 * @param command - Command to be executed
	 * @return Result of command with whitespace trimmed
	 * @throws IOException
	 */
	public static String evaluate(String command) throws IOException {
		Process p = Runtime.getRuntime().exec(command);
		String output = IOUtils.toString(p.getInputStream(), "UTF-8").trim();
		return output;
	}
	
	public static void printPropertiesTable() throws JDOMException, IOException {
		System.out.println("| Property Name | Alternate Name | Notes / Description |");
		System.out.println("| ------------- | -------------- | --------------------|");
		InputStream xmlschema = ClassLoader.getSystemResourceAsStream("property_names.xml");
		SAXBuilder xmlbuilder = new SAXBuilder();
		Document xmldocument = xmlbuilder.build(xmlschema);
		ListIterator<Element> definitions = xmldocument.getRootElement().getChildren().listIterator();
		while (definitions.hasNext()) {
			Element definition = definitions.next();
			String propname = definition.getAttributeValue("name");
			String description = definition.getChildTextNormalize("description");
			System.out.print("| ");
			System.out.print(wrapVar(propname));
			System.out.print(" | ");
			ListIterator<Element> alternates = definition.getChildren("alternate").listIterator();
			boolean first = true;

			while (alternates.hasNext()) {
				Element alternate = alternates.next();
				String altname = alternate.getAttributeValue("name");
				if (!first) System.out.print(" or ");
				System.out.print(wrapVar(altname));
				first = false;				
			}
			System.out.print(" | ");
			System.out.print(description);
			System.out.println(" |");
		}
		System.out.flush();		
	}
	
	public static String wrapVar(String name) {
		return "`" + name + "`";
	}
	
	static Document xmldocument = null;
	private static Document getXmlDocument() throws JDOMException, IOException {
		if (xmldocument == null) {
			InputStream xmlschema = ClassLoader.getSystemResourceAsStream("property_names.xml");
			SAXBuilder xmlbuilder = new SAXBuilder();
			xmldocument = xmlbuilder.build(xmlschema);			
		}
		return xmldocument;
	}
	
	/**
	 * Created to accomodate property renaming in release 3.5, this method converts
	 * any old property names to the new names.
	 * 
	 * @param oldProps Properties object with either old or new names.
	 * @param checkUnused Generates a warning if unused recognized property is detected.
	 * @return A new Properties object with new names.
	 * @throws JDOMException
	 * @throws IOException
	 */
	public static Properties replacePropertyNames(Properties oldProps, boolean checkUnused) 
			throws JDOMException, IOException {
		return replacePropertyNames(oldProps, checkUnused, false);
	}		
	
	public static Properties replacePropertyNames(Properties oldProps, boolean checkUnused, boolean print) 
			throws JDOMException, IOException {
		logger.debug(Log.INIT, "scrubPropertyNames");
		Hashtable<String,Boolean> consumed = new Hashtable<String,Boolean>();
		Properties newProps = new Properties();
		Document xmldocument = getXmlDocument();
		ListIterator<Element> definitions = xmldocument.getRootElement().getChildren().listIterator();
		while (definitions.hasNext()) {
			Element definition = definitions.next();
			String propname = definition.getAttributeValue("name");
			logger.debug(Log.INIT, "processing " + propname);
			boolean found = false;
			if (oldProps.containsKey(propname)) {
				found = true;
				newProps.setProperty(propname, oldProps.getProperty(propname));
				consumed.put(propname, Boolean.TRUE);
			}
			else {
				// Look for it under an alternate name
				ListIterator<Element> alternates = definition.getChildren("alternate").listIterator();
				while (alternates.hasNext()) {
					Element alternate = alternates.next();
					String altname = alternate.getAttributeValue("name");
					logger.debug(Log.INIT, "alternate " + altname);
					if (oldProps.containsKey(altname)) {
						found = true;
						newProps.setProperty(propname, oldProps.getProperty(altname));
						consumed.put(altname, Boolean.TRUE);
					}				
				}
			}
			if (found && print) 
				System.out.println(String.format("%s=%s", propname, newProps.getProperty(propname)));
		}
		if (checkUnused) {
			for (String propname : oldProps.stringPropertyNames()) {
				if (!consumed.containsKey(propname))
					logger.warn("Unrecognized property: " + propname);
			}
		}
		return newProps;		
	}
	
}
