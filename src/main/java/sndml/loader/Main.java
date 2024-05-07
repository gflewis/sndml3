package sndml.loader;

import java.io.File;
import java.nio.file.Files;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.*;
import sndml.server.AgentHttpServer;
import sndml.servicenow.RecordKey;
import sndml.util.Log;

public class Main {

	static final Logger logger = LoggerFactory.getLogger(Main.class);
	static ConnectionProfile profile;
	static boolean agent_mode = false;

	static Options options = new Options();	
	static final Option optProfile = 
			Option.builder("p").longOpt("profile").required(true).hasArg(true).
			desc("Property file (required)").build();
	static final Option optTable = 
			Option.builder("t").longOpt("table").required(false).hasArg(true).
			desc("Table name").build();
	static final Option optFilter =
			Option.builder("f").longOpt("filter").required(false).hasArg(true).
			desc("Encoded query for use with --table").build();
	static final Option optYaml = 
			Option.builder("y").longOpt("yaml").required(false).hasArg(true).
			desc("YAML config file (required)").build();
	static final Option optJobRun = 
			Option.builder("jobrun").longOpt("jobrun").required(false).hasArg(true).
			desc("sys_id of job").build();
	static final Option optDaemon =
			Option.builder("daemon").longOpt("daemon").required(false).hasArg(false).
			desc("Run as daemon/service").build();
	static final Option optScan =
			Option.builder("scan").longOpt("scan").required(false).hasArg(false).
			desc("Run the deamon scanner once").build();
	static final Option optServer =
			Option.builder("server").longOpt("server").required(false).hasArg(false).
			desc("Run as server").build();
	
	/**
	 * This is the main class invoked from the JAR file.
	 */
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		options.addOption(optProfile);
		options.addOption(optTable);
		options.addOption(optFilter);
		options.addOption(optYaml);
		options.addOption(optJobRun);
		options.addOption(optScan);
		options.addOption(optDaemon);
		options.addOption(optServer);
				
		CommandLine cmd = new DefaultParser().parse(options,  args);
		int cmdCount = 0;
		if (cmd.hasOption(optYaml)) cmdCount += 1;
		if (cmd.hasOption(optTable)) cmdCount += 1;
		if (cmd.hasOption(optJobRun)) cmdCount += 1;
		if (cmd.hasOption(optScan)) cmdCount += 1;		
		if (cmd.hasOption(optDaemon)) cmdCount += 1;
		if (cmd.hasOption(optServer)) cmdCount += 1;
		if (cmdCount != 1) 
			throw new CommandOptionsException(
				String.format("Must specify exactly one of: --%s, --%s, --%s or --%s",
						optYaml.getLongOpt(),optTable.getLongOpt(),
						optScan.getLongOpt(), optDaemon.getLongOpt()));
		String profileName = cmd.getOptionValue("p");
		profile = new ConnectionProfile(new File(profileName));

		if (cmd.hasOption(optTable)) {
			// Simple Table Loader
			String tableName = cmd.getOptionValue("t");
			String filter = cmd.getOptionValue("f");
			DatabaseConnection database = profile.newDatabaseConnection();
			SimpleTableLoader tableLoader = new SimpleTableLoader(profile, database, tableName, filter);
			tableLoader.call();
		}
		else {
			if (cmd.hasOption(optFilter))
				throw new CommandOptionsException("--filter only valid when used with --table");
		}		
		if (cmd.hasOption(optYaml)) {
			// YAML File Loader
			String yamlFileName = cmd.getOptionValue("y");
			File yamlFile = new File(yamlFileName);
			String yamlText = Files.readString(yamlFile.toPath());
			logger.info(Log.INIT, yamlFileName + ":\n" + yamlText.trim());
			YamlLoader loader = new YamlLoader(profile, yamlFile);
			loader.loadTables();
		}
		if (cmd.hasOption(optDaemon)) {
			// Daemon
			agent_mode = true;
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		if (cmd.hasOption(optScan)) {
			// Scan once
			agent_mode = true;
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
		if (cmd.hasOption(optJobRun)) {
			// Run a single job
			agent_mode = true;
			String sys_id = cmd.getOptionValue("jobrun");
			RecordKey jobkey = new RecordKey(sys_id);
			SingleJobRunner jobRunner = new SingleJobRunner(profile, jobkey);
			jobRunner.run();			
		}
		if (cmd.hasOption(optServer)) {
			// Server
			AgentHttpServer server = new AgentHttpServer(profile);
			server.start();
		}
	}
	
	public static ConnectionProfile getProfile() {
		return profile;
	}
	
	/**
	 * Return true if this process is connected to a scoped app 
	 * in the ServiceNow instance. 
	 * 
	 * @return true if using scoped app, otherwise false
	 */
	public static boolean isAgent() {
		return agent_mode;
	}
	
	public String getAgentName() {
		return profile.getAgentName();
	}
}
