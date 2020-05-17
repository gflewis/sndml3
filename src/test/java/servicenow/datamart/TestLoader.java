package servicenow.datamart;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;

import servicenow.api.TestingException;
import servicenow.api.WriterMetrics;

public class TestLoader {

	final LoaderConfig config;
	ArrayList<LoaderJob> jobs = null;	

	public TestLoader(String text) {
		StringReader reader = new StringReader(text);
		try {
			config = new LoaderConfig(reader);
		} catch (ConfigParseException | IOException e) {
			throw new TestingException(e);
		}
	}
	
	public TestLoader(File file) {
		try {
			config = new LoaderConfig(file);
		} catch (ConfigParseException | IOException e) {
			throw new TestingException(e);
		}		
	}
	
	public WriterMetrics load() {
		Loader loader = new Loader(config);
		jobs = loader.jobs;
		LoaderJob lastJob = jobs.get(jobs.size() - 1);
		try {
			loader.loadTables();
		} catch (SQLException | IOException | InterruptedException e) {
			throw new TestingException(e);
		}
		return lastJob.getMetrics();		
	}
	
	public LoaderJob getJob(int index) {
		return jobs.get(index);
	}
	
	public WriterMetrics getMetrics(int index) {
		return getJob(index).getMetrics();
	}

}
