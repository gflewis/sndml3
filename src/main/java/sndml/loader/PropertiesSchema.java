package sndml.loader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.ListIterator;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.util.Log;
import sndml.util.ResourceException;

/**
 * To print out list of properties
 * <code>
 *   --markdown
 * </code>
 * 
 * To validate properties file
 * <code>
 *   --validate filename
 * </code>
 * 
 */	
public class PropertiesSchema {

	final private Document xmlDocument;
	final private Properties defaultValues;
	final private TreeSet<String> validNames;	
	
	private static final Logger logger = LoggerFactory.getLogger(PropertiesSchema.class);
	
	public static void main(String[] args) throws Exception {
		Option optValidate = 
			Option.builder("v").longOpt("validate").required(false).hasArg(true).
			desc("Validate property file").build();
		Option optHelp =
			Option.builder("m").longOpt("markdown").required(false).hasArg(false).
			desc("Print markdown table of available properties").build();		
		Options options = new Options();
		options.addOption(optValidate);
		options.addOption(optHelp);
		CommandLine cmdline = new DefaultParser().parse(options, args);
		PropertiesSchema schema = new PropertiesSchema();
		if (cmdline.hasOption(optHelp)) {
			schema.printPropertiesTable();
		}
		else if (cmdline.hasOption(optValidate)) {
			String filename = cmdline.hasOption(optValidate) ? 
					cmdline.getOptionValue(optValidate) : null;
			Properties properties = new Properties();
			properties.load(new FileInputStream(filename));
			properties = schema.replacePropertyNames(properties, true, true);
		}
		else 
			throw new CommandOptionsException(
				String.format("Must specify --%s or --%s",
					optHelp.getLongOpt(), optValidate.getLongOpt()));			
	}
	
	public PropertiesSchema() throws ResourceException {
		try {
			InputStream xmlschema = ClassLoader.getSystemResourceAsStream("property_names.xml");
			SAXBuilder xmlbuilder = new SAXBuilder();
			this.xmlDocument = xmlbuilder.build(xmlschema);
		} catch (JDOMException | IOException e) {
			throw new ResourceException(e);
		}

		defaultValues = new Properties();
		validNames = new TreeSet<String>();
		ListIterator<Element> definitions = xmlDocument.getRootElement().getChildren().listIterator();
		while (definitions.hasNext()) {
			Element definition = definitions.next();
			String propname = definition.getAttributeValue("name");
			assert propname != null;
			validNames.add(propname);
			String defaultvalue = definition.getChildTextNormalize("default");
			if (defaultvalue != null)
				defaultValues.setProperty(propname, defaultvalue);
		}				
	}

	/**
	 * Returns the names of all properties
	 */
	public TreeSet<String> getValidNames() {
		return this.validNames;
	}
	
	/**
	 * Returns default values for all properties defined in property_names.xml.
	 */
	public Properties getDefaultValues() {
		return this.defaultValues;
	}
	
	public boolean hasName(String name) {
		return this.validNames.contains(name);
	}
	
	public boolean hasDefault(String name) {
		return this.defaultValues.containsKey(name);
	}
	
	public String getDefault(String name) {
		return hasDefault(name) ? defaultValues.getProperty(name) : null;
	}
	
	public void printPropertiesTable() throws JDOMException, IOException {
		System.out.println("| Property Name | Alternate Name(s) | Notes / Description |");
		System.out.println("| ------------- | ----------------- | --------------------|");
		ListIterator<Element> definitions = xmlDocument.getRootElement().getChildren().listIterator();
		while (definitions.hasNext()) {
			Element definition = definitions.next();
			String propname = definition.getAttributeValue("name");
			String hidden = definition.getAttributeValue("hidden");
			if (!"true".equals(hidden)) {
				String description = definition.getChildTextNormalize("description");
				System.out.print("| ");
				System.out.print(wrapVar(propname));
				System.out.print(" | ");
				ListIterator<Element> alternates = definition.getChildren("alternate").listIterator();
				boolean first = true;
				while (alternates.hasNext()) {
					Element alternate = alternates.next();
					String altname = alternate.getAttributeValue("name");
					if (!first) System.out.print(", ");
					System.out.print(altname);
					first = false;				
				}
				System.out.print(" | ");
				System.out.print(description);
				System.out.println(" |");				
			}
		}
		System.out.flush();		
	}
	
	public String wrapVar(String name) {
		return "`" + name + "`";
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
	public Properties replacePropertyNames(Properties oldProps, boolean checkUnused) 
			throws JDOMException, IOException {
		return replacePropertyNames(oldProps, checkUnused, false);
	}		
	
	public Properties replacePropertyNames(Properties oldProps, boolean checkUnused, boolean print) 
			throws JDOMException, IOException {
		logger.debug(Log.INIT, "replacePropertyNames");
		Hashtable<String,Boolean> consumed = new Hashtable<String,Boolean>();
		Properties newProps = new Properties();
		ListIterator<Element> definitions = xmlDocument.getRootElement().getChildren().listIterator();
		while (definitions.hasNext()) {
			Element definition = definitions.next();
			String propname = definition.getAttributeValue("name");
			logger.trace(Log.INIT, "processing " + propname);
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
					logger.trace(Log.INIT, "alternate " + altname);
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
					logger.warn(Log.INIT, "Unrecognized property: " + propname);
			}
		}
		return newProps;		
	}	
}
