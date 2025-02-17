package sndml.agent;

import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;

import sndml.util.Log;

public class AppStatusQueue extends LinkedBlockingQueue<AppStatusPayload> {

	private static final long serialVersionUID = 1L;

	/**
	 * Wait for this queue to become empty.
	 * Designed to be used by publisher at end of job.
	 */
	void flush(Logger logger) {
		if (peek() != null) {
			logger.info(Log.FINISH, "Flushing status queue");
			while (peek() != null) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// ignore interrupt
				}				
			}
		}
	}
}
