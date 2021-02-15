package sndml.datamart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.servicenow.*;

public class ActionRunner implements Runnable {
	
	final Session session;
	final Database db;
	final JobConfig config;
	final Key runKey;
	final String jobname;
	final Thread mainThread;
	final LoaderJob job;
	final AppRunLogger runLogger;
	final Logger logger = LoggerFactory.getLogger(ActionRunner.class);
	
	public ActionRunner(ConnectionProfile profile, JobConfig config) {
		this.session = profile.getSession();
		this.db = profile.getDatabase();
		this.config = config;
		this.runKey = config.getSysId();
		this.jobname = config.getName();
		assert runKey != null;
		assert runKey.isGUID();
		this.mainThread = Thread.currentThread();
		this.runLogger = new AppRunLogger(profile, session, runKey);
		this.job = new LoaderJob(session, db, config, runLogger);
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName(config.getNumber());
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
	
}
