package sndml.datamart;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.daemon.*;
import sndml.util.Log;

public class Main {

	static final Logger logger = LoggerFactory.getLogger(Main.class);
	
	/**
	 * This is the main class invoked from the JAR file.
	 */
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required(true).hasArg(true).
				desc("Property file (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(false).hasArg(true).
				desc("Table name").build());
		options.addOption(Option.builder("y").longOpt("yaml").required(false).hasArg(true).
				desc("YAML config file (required)").build());
		options.addOption(Option.builder("j").longOpt("job").required(false).hasArg(true).
				desc("sys_id of job").build());
		options.addOption(Option.builder("daemon").longOpt("daemon").required(false).hasArg(false).
				desc("Run as daemon/service").build());
		options.addOption(Option.builder("server").longOpt("server").required(false).hasArg(false).
				desc("Run as server").build());		
		options.addOption(Option.builder("scan").longOpt("scan").required(false).hasArg(false).
				desc("Run the deamon scanner once").build());
		options.addOption(Option.builder("f").longOpt("filter").required(false).hasArg(true).
				desc("Encoded query for use with --table").build());
		CommandLine cmd = new DefaultParser().parse(options,  args);
		int cmdCount = 0;
		if (cmd.hasOption("y")) cmdCount += 1;
		if (cmd.hasOption("t")) cmdCount += 1;
		if (cmd.hasOption("j")) cmdCount += 1;
		if (cmd.hasOption("daemon")) cmdCount += 1;
		if (cmd.hasOption("scan")) cmdCount += 1;		
		if (cmdCount != 1) 
			throw new CommandOptionsException(
				"Must specify exactly one of: --yaml, --table, --job, --daemon or --scan");
		String profileName = cmd.getOptionValue("p");
		ConnectionProfile profile = new ConnectionProfile(new File(profileName));

		if (cmd.hasOption("t")) {
			// Simple Table Loader
			String tableName = cmd.getOptionValue("t");
			String filter = cmd.getOptionValue("f");
			Database database = profile.getDatabase();
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
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		if (cmd.hasOption("server")) {
			// Server
			AgentServer server = new AgentServer(profile);
			server.start();
		}
		if (cmd.hasOption("scan")) {
			// Scan once
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
	}
}
