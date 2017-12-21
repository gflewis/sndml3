package servicenow.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.core.*;

public class LoaderConfig extends Config {

	final DateTime start = DateTime.now();
	
	private Map root;
	private Integer threads = 0;
	private String name = "loader";
	private File metricsFile;
	
	private final java.util.List<TableLoaderConfig> tables = 
			new java.util.ArrayList<TableLoaderConfig>();

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public LoaderConfig(Table table) throws IOException, ConfigParseException {
		tables.add(new TableLoaderConfig(table));		
	}
	
	public LoaderConfig(File configFile) throws IOException, ConfigParseException {
		this(new FileReader(configFile), defaultMetricsFile(configFile));
	}

	public LoaderConfig(Reader reader, File defaultMetricsFile) throws IOException, ConfigParseException {
		this.metricsFile = defaultMetricsFile;
		root = parseDocument(reader);		
		logger.info(Log.INIT, "\n" + parser.dump(root).trim());
		for (String key : root.keySet()) {
		    Object val = root.get(key);
			switch (key.toLowerCase()) {
			case "name" : name = val.toString(); break;
			case "threads" : threads = asInteger(val); break;
			case "metrics" : metricsFile = new File(val.toString()); break;
			case "tables" : 
				for (Object job : toList(val)) {
					this.tables.add(new TableLoaderConfig(this, job));
				}
				break;
		    	default:
		    		throw new ConfigParseException("Not recognized: " + key);
			}
		}		
	}
	
	String getString(String propName) {
		assert root != null;
		return root.getString(propName);
	}
	
	@Deprecated
	private static File defaultMetricsFile(File configFile) {
		if (configFile == null) return null;
		String path = configFile.getPath();
		// remove old extension
		path = path.replaceFirst("\\.\\w$",  "");
		// add new extension
		path = path + ".metrics";
		return new File(path);
	}
	
	public java.util.List<TableLoaderConfig> getJobs() {
		return this.tables;
	}

	public String getName() {
		return this.name;
	}
	
	public int getThreads() {
		return this.threads==null ? 0 : this.threads.intValue();
	}
		
	public File getMetricsFile() {
		return metricsFile;
	}
	
	/**
	 * Return the DateTime that this object was initialized.
	 */
	public DateTime getStart() {
		return start;
	}
			
}
