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
import sndml.servicenow.Log;

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
		options.addOption(Option.builder("daemon").longOpt("daemon").required(false).hasArg(false).
				desc("Run as daemon").build());
		options.addOption(Option.builder("server").longOpt("server").required(false).hasArg(false).
				desc("Run as server").build());
		options.addOption(Option.builder("scan").longOpt("scan").required(false).hasArg(false).
				desc("Run the deamon scanner once").build());
		CommandLine cmd = new DefaultParser().parse(options,  args);
		String profileName = cmd.getOptionValue("p");
		ConnectionProfile profile = new ConnectionProfile(new File(profileName));
		int cmdCount = 0;
		if (cmd.hasOption("y")) cmdCount += 1;
		if (cmd.hasOption("t")) cmdCount += 1;
		if (cmd.hasOption("daemon")) cmdCount += 1;
		if (cmd.hasOption("server")) cmdCount += 1;
		if (cmd.hasOption("scan")) cmdCount += 1;		
		if (cmdCount != 1) 
			throw new CommandOptionsException(
				"Must specify one of: --yaml, --table, --daemon, --server or --scan");
		if (cmd.hasOption("t")) {
			// Simple Table Loader
			String tableName = cmd.getOptionValue("t");
			Database database = profile.getDatabase();
			SimpleTableLoader tableLoader = new SimpleTableLoader(profile, tableName, database);
			tableLoader.call();
		}
		else if (cmd.hasOption("y")) {
			// YAML file
			String yamlFileName = cmd.getOptionValue("y");
			File yamlFile = new File(yamlFileName);
			String yamlText = Files.readString(yamlFile.toPath());
			logger.info(Log.INIT, yamlFileName + ":\n" + yamlText.trim());
			FileReader reader = new FileReader(new File(yamlFileName));
			ConfigFactory factory = new ConfigFactory();
			LoaderConfig config = factory.loaderConfig(profile, reader);
			Loader loader = new Loader(profile, config);			
			loader.loadTables();
		}
		else if (cmd.hasOption("daemon")) {
			// Daemon
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Starting daemon: " + AgentDaemon.getAgentName());
			daemon.runForever();
		}
		else if (cmd.hasOption("server")) {
			// Server
			AgentServer server = new AgentServer(profile);
			server.start();
		}
		else if (cmd.hasOption("scan")) {
			// Scan once
			AgentDaemon daemon = new AgentDaemon(profile);
			logger.info(Log.INIT, "Scanning agent: " + AgentDaemon.getAgentName());
			daemon.scanOnce();
		}
	}
}
