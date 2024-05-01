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
import sndml.servicenow.RecordKey;
import sndml.util.Log;

public class Main {

	static final Logger logger = LoggerFactory.getLogger(Main.class);
	static Options options;
	static ConnectionProfile profile;
	static boolean agent_mode = false;
	
	/**
	 * This is the main class invoked from the JAR file.
	 */
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		
		options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required(true).hasArg(true).
				desc("Property file (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(false).hasArg(true).
				desc("Table name").build());
		options.addOption(Option.builder("y").longOpt("yaml").required(false).hasArg(true).
				desc("YAML config file (required)").build());
		options.addOption(Option.builder("job").longOpt("job").required(false).hasArg(true).
				desc("sys_id of job").build());
		options.addOption(Option.builder("daemon").longOpt("daemon").required(false).hasArg(false).
				desc("Run as daemon/service").build());
		options.addOption(Option.builder("scan").longOpt("scan").required(false).hasArg(false).
				desc("Run the deamon scanner once").build());
		options.addOption(Option.builder("f").longOpt("filter").required(false).hasArg(true).
				desc("Encoded query for use with --table").build());
		// TODO Coming Soon: --server
		/*
		options.addOption(Option.builder("server").longOpt("server").required(false).hasArg(false).
				desc("Run as server").build());
		*/		
		CommandLine cmd = new DefaultParser().parse(options,  args);
		int cmdCount = 0;
		if (cmd.hasOption("y")) cmdCount += 1;
		if (cmd.hasOption("t")) cmdCount += 1;
		if (cmd.hasOption("job")) cmdCount += 1;
		if (cmd.hasOption("daemon")) cmdCount += 1;
		if (cmd.hasOption("scan")) cmdCount += 1;		
		if (cmdCount != 1) 
			throw new CommandOptionsException(
				"Must specify exactly one of: --yaml, --table, --job, --daemon or --scan");
		String profileName = cmd.getOptionValue("p");
		profile = new ConnectionProfile(new File(profileName));

		if (cmd.hasOption("t")) {
			// Simple Table Loader
			String tableName = cmd.getOptionValue("t");
			String filter = cmd.getOptionValue("f");
			DatabaseConnection database = profile.newDatabaseConnection();
			SimpleTableLoader tableLoader = new SimpleTableLoader(profile, database, tableName, filter);
			tableLoader.call();
		}
		else {
			if (cmd.hasOption("f"))
				throw new CommandOptionsException("--filter only valid when used with --table");
		}		
		if (cmd.hasOption("y")) {
			// YAML File Loader
			String yamlFileName = cmd.getOptionValue("y");
			File yamlFile = new File(yamlFileName);
			String yamlText = Files.readString(yamlFile.toPath());
			logger.info(Log.INIT, yamlFileName + ":\n" + yamlText.trim());
			YamlLoader loader = new YamlLoader(profile, yamlFile);
			loader.loadTables();
		}
		if (cmd.hasOption("daemon")) {
			// Daemon
			agent_mode = true;
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		if (cmd.hasOption("scan")) {
			// Scan once
			agent_mode = true;
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
		if (cmd.hasOption("job")) {
			// Run a single job
			agent_mode = true;
			String sys_id = cmd.getOptionValue("job");
			RecordKey jobkey = new RecordKey(sys_id);
			SingleJobRunner jobRunner = new SingleJobRunner(profile, jobkey);
			jobRunner.run();			
		}
		/*
		if (cmd.hasOption("server")) {
			// Server
			AgentServer server = new AgentServer(profile);
			server.start();
		}
		*/
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
