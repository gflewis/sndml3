package sndml.agent;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;

import sndml.loader.ConnectionProfile;
import sndml.loader.Main;
import sndml.loader.Resources;
import sndml.servicenow.RecordKey;
import sndml.util.Log;
import sndml.util.ResourceException;

//Q: Why does this class exist? 
//A: Because many of the classes in the package are not public. 

public class AgentMain extends Main {

	static String agentName;
	static RecordKey agentKey;
	static final Logger logger = Log.getLogger(AgentMain.class);
	
	public static void main(CommandLine cmd, Resources resources) throws Exception {
		// Note: resources is actually a static protected variable in Main;
		// thus we could access it even if it were not a parameter		
		assert resources != null;
		agentName = resources.getProfile().getAgentName();
		if (agentName == null) 
			throw new AssertionError(
				"No value for property: " + ConnectionProfile.APP_AGENT_PROPERTY);
		AppSession appSession = resources.getAppSession();
		agentKey = appSession.getAgentKey();
		assert agentKey != null;
				
		if (cmd.hasOption(optScan)) {
			// Scan once
			AgentDaemon daemon = new AgentDaemon(resources);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
		else if (cmd.hasOption(optDaemon)) {
			// Scan forever
			AgentDaemon daemon = new AgentDaemon(resources);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		else if (cmd.hasOption(optJobRun)) {
			// Run a single job
			String sys_id = cmd.getOptionValue("jobrun");
			RecordKey jobkey = new RecordKey(sys_id);
			AppConfigFactory factory = new AppConfigFactory(resources);
			AppJobConfig jobconfig = factory.appJobConfig(jobkey);
			AppJobRunner jobrunner = new AppJobRunner(resources, jobconfig);			
			jobrunner.call();			
		}
		else if (cmd.hasOption(optServer)) {
			// Run as an HTTP Server
			AgentHttpServer server = new AgentHttpServer(resources);
			server.start();
		}
		else {
			throw new IllegalStateException(); // should never be here
		}
		
	}

	RecordKey getAgentKey() {
		return agentKey;
	}
	
	/**
	 * Return value of profile property "app.agent" or null if not defined
	 */
	public static String getAgentName() {
		return Main.profile.getAgentName();
	}
	
	static void writePidFile() throws ResourceException {
        ProcessHandle processHandle = ProcessHandle.current();
        String pidFileName = Main.profile.getPidFileName();
		long pid = processHandle.pid();
		if (pidFileName == null) {
			logger.info(Log.INIT, String.format("writePidFile pid=%d", pid));			
		}
		else {
			File pidFile = new File(pidFileName);
			logger.info(Log.INIT, String.format(
				"init pid=%d pidfile=%s", pid, pidFile.getAbsolutePath()));
			PrintWriter pidWriter;
			try {
				pidWriter = new PrintWriter(pidFile);
				pidWriter.println(pid);
				pidWriter.close();
			} catch (IOException e) {
				throw new ResourceException(
					"Unable to write pidfile: " + pidFileName);
				
			}
		}		
	}
	
}
