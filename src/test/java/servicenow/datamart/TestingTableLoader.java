package servicenow.datamart;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;

import servicenow.api.TestingException;
import servicenow.api.WriterMetrics;

@Deprecated
public class TestingTableLoader {

	final LoaderConfig config;
	ArrayList<LoaderJob> jobs = null;	

	public TestingTableLoader(String text) {
		StringReader reader = new StringReader(text);
		try {
			config = new LoaderConfig(reader, null);
		} catch (ConfigParseException e) {
			throw new TestingException(e);
		}
	}
	
	public TestingTableLoader(File file) {
		try {
			config = new LoaderConfig(file, null);
		} catch (ConfigParseException | IOException e) {
			throw new TestingException(e);
		}		
	}
	
	public WriterMetrics load(ConnectionProfile profile) {
		Loader loader = new Loader(profile, config);
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
