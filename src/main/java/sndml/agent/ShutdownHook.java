package sndml.agent;

import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import sndml.loader.Resources;
import sndml.util.Log;
import sndml.util.Metrics;

public class ShutdownHook extends Thread {

	final WorkerPool workerPool;
	final AppStatusLogger statusLogger;
	final Resources resources;
	
	final Logger logger = Log.getLogger(ShutdownHook.class);
		
	ShutdownHook(Resources resources) {
		this.resources = resources;
		this.workerPool = resources.getWorkerPool();
		this.statusLogger = new AppStatusLogger(resources.getAppSession());
		this.setName(this.getClass().getSimpleName());
	}
		
	void cancelAll() {
		int count = 0;
		List<WorkerEntry> list = workerPool.activeTasks();
		for (WorkerEntry entry : list) {
			Future<Metrics> future = entry.future;
			if (!future.isCancelled() && !future.isDone()) {
				logger.info(Log.FINISH, String.format("Cancel %s", entry.number));
				future.cancel(true);
				statusLogger.cancelJob(entry.key, null);
				count += 1;
			}	
		}
		if (count > 0)
			logger.info(Log.FINISH, String.format("%d job(s) cancelled", count));
		else 
			logger.info(Log.FINISH, "No active jobs");
	}
	
	@Override
	public void run() {
		Log.setGlobalContext();
		logger.info(Log.FINISH, "ShutdownHook invoked");
		cancelAll();
		logger.info(Log.FINISH, "ShutdownHook complete");
		Log.shutdown();
	}
	
}
