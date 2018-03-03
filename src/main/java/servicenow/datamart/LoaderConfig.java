package servicenow.datamart;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.*;

public class LoaderConfig extends Config {

	final DateTime start = DateTime.now();
	
	Map root;
	Integer threads = 0;
	File metricsFile = null;
	
	private final java.util.List<TableConfig> tables = 
			new java.util.ArrayList<TableConfig>();

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	LoaderConfig(Table table) throws IOException, ConfigParseException {
		tables.add(new TableConfig(table));		
	}
	
	LoaderConfig(File configFile) throws IOException, ConfigParseException {
		this(new FileReader(configFile));
	}
	
	LoaderConfig(Reader reader) throws IOException, ConfigParseException {
		Globals.setLoaderConfig(this);
		root = parseDocument(reader);		
		logger.info(Log.INIT, "\n" + parser.dump(root).trim());
		for (String key : root.keySet()) {
		    Object val = root.get(key);
			switch (key.toLowerCase()) {
			case "threads" : threads = asInteger(val); break;
			case "metrics" : metricsFile = new File(val.toString()); break;
			case "tables" : 
				for (Object job : toList(val)) {
					this.tables.add(new TableConfig(this, job));
				}
				break;
		    	default:
		    		throw new ConfigParseException("Not recognized: " + key);
			}
		}
	}
	
	String getString(String key) {
		assert root != null;
		return root.getString(key);
	}
		
	java.util.List<TableConfig> getJobs() {
		return this.tables;
	}
	
	/*
	 * Used for JUnit tests
	 */
	TableConfig getJobByName(String name) {
		assert name != null;
		for (TableConfig job : tables) {
			if (name.equals(job.getName())) return job;
		}
		return null;
	}
	
	int getThreads() {
		return this.threads==null ? 0 : this.threads.intValue();
	}
	
	File getMetricsFile() {
		return metricsFile;
	}
	
	/**
	 * Return the DateTime that this object was initialized.
	 */
	DateTime getStart() {
		return start;
	}
			
}
