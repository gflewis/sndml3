package sndml.datamart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class ActionRunner implements Runnable {
	
	static Logger logger = LoggerFactory.getLogger(ActionRunner.class);

	final Session session;
	final Database db;
	final JobConfig settings;
	final Key key;
	final String jobname;
	final Thread mainThread;
	final LoaderJob job;
	final AppRunLogger runLogger;
	
	public ActionRunner(Session session, ConnectionProfile profile, JobConfig settings) {
		this.session = session;
		this.db = profile.getDatabase();
		this.settings = settings;
		this.key = settings.getId();
		this.jobname = settings.getName();
		assert key != null;
		assert key.isGUID();
		this.mainThread = Thread.currentThread();
		this.job = new LoaderJob(session, db, settings);
		runLogger = new AppRunLogger(logger, profile, session);
		runLogger.setRunKey(this.key);		
	}
	
	@Override
	public void run() {
		try {
			runLogger.setStatus("running");
			job.call();
			runLogger.setStatus("complete");
		}
		catch (Exception e) {
			Log.setJobContext(this.jobname);
			logger.error(Log.RESPONSE, e.toString(), e);
			e.printStackTrace();
			mainThread.interrupt();				
		}
	}
	
	/*
	private void setRunStatus(String status) throws IOException {
		ObjectNode body = JsonNodeFactory.instance.objectNode();
		body.put("sys_id", this.key.toString());
		body.put("status", status);
		JsonRequest request = new JsonRequest(session, putRunStatus, HttpMethod.PUT, body);
		request.execute();
	}
	*/

}
