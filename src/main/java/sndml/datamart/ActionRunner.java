package sndml.datamart;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.Key;
import sndml.servicenow.Log;
import sndml.servicenow.Session;
//import sndml.servicenow.Table;

public class ActionRunner implements Runnable {
	
	static Logger logger = LoggerFactory.getLogger(ActionRunner.class);

	final Session session;
	final Database db;
	final JobConfig settings;
	final Key key;
	final String jobname;
	final URI putRunStatus;
	final Thread mainThread;
	final LoaderJob job;
	
	public ActionRunner(Session session, ConnectionProfile profile, JobConfig settings) {
		this.session = session;
		this.db = profile.getDatabase();
		this.settings = settings;
		this.key = settings.getId();
		this.jobname = settings.getNumber();
		assert key != null;
		assert key.isGUID();
		String putRunStatusPath = profile.getProperty(
				"loader.api.putrunstatus",
				"api/x_108443_sndml/putrunstatus");
		this.putRunStatus = session.getURI(putRunStatusPath);
		this.mainThread = Thread.currentThread();
		this.job = new LoaderJob(session, db, settings);
		
	}
	
	@Override
	public void run() {
		try {
			setRunStatus("running");
			job.call();
			setRunStatus("complete");
		}
		catch (Exception e) {
			Log.setJobContext(this.jobname);
			logger.error(Log.RESPONSE, e.toString(), e);
			e.printStackTrace();
			mainThread.interrupt();				
		}
	}
	
	private void setRunStatus(String status) throws IOException {
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", this.key.toString());
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		request.execute();
	}	

}
