package sndml.loader;

import java.io.File;
import java.nio.file.Files;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.agent.AgentMain;
import sndml.servicenow.EncodedQuery;
import sndml.servicenow.RecordKey;
import sndml.servicenow.Table;
import sndml.util.Log;

/**
 * This is the main class invoked from the JAR file. This is a singleton class.
 */
public class Main {

	static protected final Thread mainThread = Thread.currentThread();		
	static protected ConnectionProfile profile;
	static protected Resources resources;
	static private boolean requiresApp = false;
	
	static Options options = new Options();	
	static final protected Option optProfile = 
			Option.builder("p").longOpt("profile").required(true).hasArg(true).
			desc("Property file (required)").build();
	static final protected Option optTable = 
			Option.builder("t").longOpt("table").required(false).hasArg(true).
			desc("Table name").build();
	static final protected Option optFilter =
			Option.builder("f").longOpt("filter").required(false).hasArg(true).
			desc("Encoded query for use with --table").build();
	static final protected Option optSysID =
			Option.builder("sys_id").longOpt("sys_id").required(false).hasArg(true).
			desc("sys_id of record for use with --table").build();
	static final protected Option optYaml = 
			Option.builder("y").longOpt("yaml").required(false).hasArg(true).
			desc("YAML config file (required)").build();
	static final protected Option optJobRun = 
			Option.builder("jobrun").longOpt("jobrun").required(false).hasArg(true).
			desc("sys_id of job").build();
	static final protected Option optDaemon =
			Option.builder("daemon").longOpt("daemon").required(false).hasArg(false).
			desc("Run as daemon/service").build();
	static final protected Option optScan =
			Option.builder("scan").longOpt("scan").required(false).hasArg(false).
			desc("Run the deamon scanner once").build();
	static final protected Option optServer =
			Option.builder("server").longOpt("server").required(false).hasArg(false).
			desc("Run as server").build();

	static protected String agentName;
	static private Logger logger;
		
	public static void main(String[] args) throws Exception {
		options.addOption(optProfile);
		options.addOption(optTable);
		options.addOption(optFilter);
		options.addOption(optSysID);
		options.addOption(optYaml);
		options.addOption(optJobRun);
		options.addOption(optScan);
		options.addOption(optDaemon);
		options.addOption(optServer);
				
		CommandLine cmd = new DefaultParser().parse(options,  args);
		int cmdCount = 0;
		if (cmd.hasOption(optYaml))   cmdCount += 1;
		if (cmd.hasOption(optTable))  cmdCount += 1;
		if (cmd.hasOption(optJobRun)) { cmdCount += 1; requiresApp = true; }
		if (cmd.hasOption(optScan))   { cmdCount += 1; requiresApp = true; }	
		if (cmd.hasOption(optDaemon)) { cmdCount += 1; requiresApp = true; }
		if (cmd.hasOption(optServer)) { cmdCount += 1; requiresApp = true; }
		if (cmdCount != 1) 
			throw new CommandOptionsException(
				String.format("Must specify exactly one of: --%s, --%s, --%s, --%s, --%s or --%s",
						optYaml.getLongOpt(),optTable.getLongOpt(),
						optScan.getLongOpt(), optDaemon.getLongOpt(),
						optServer.getLongOpt(), optJobRun.getLongOpt()));
		String profileName = cmd.getOptionValue(optProfile);
		profile = new ConnectionProfile(new File(profileName));
		resources = new Resources(profile, requiresApp);
		agentName = profile.getAgentName();
		Log.setGlobalContext(agentName);
		Main.logger = LoggerFactory.getLogger(Main.class);

		if (cmd.hasOption(optTable)) {
			// Simple Table Loader
			ReaderSession session = resources.getReaderSession();
			String tableName = cmd.getOptionValue(optTable);
			String filter = cmd.getOptionValue(optFilter);
			String sys_id = cmd.getOptionValue(optSysID);
			Table table = session.table(tableName);
			EncodedQuery query = new EncodedQuery(table, filter);
			logger.debug(Log.INIT, "table=" + cmd.getOptionValue(optTable));
			logger.debug(Log.INIT, "sys_id=" + cmd.getOptionValue(optSysID));
			RecordKey docKey = cmd.hasOption(optSysID) ? new RecordKey(sys_id) : null;
			SimpleTableLoader tableLoader = (docKey == null) ?
					new SimpleTableLoader(resources, table, query) : 
					new SimpleTableLoader(resources, table, docKey);			
			tableLoader.call();
			shutdown();
		}
		else {
			if (cmd.hasOption(optFilter))
				throw new CommandOptionsException("--filter only valid when used with --table");
			if (cmd.hasOption(optSysID))
				throw new CommandOptionsException("--sys_id only valid when used with --table");
		}		
		if (cmd.hasOption(optYaml)) {
			// YAML File Loader
			String yamlFileName = cmd.getOptionValue(optYaml);
			File yamlFile = new File(yamlFileName);
			String yamlText = Files.readString(yamlFile.toPath());
			logger.info(Log.INIT, yamlFileName + ":\n" + yamlText.trim());
			YamlLoader loader = new YamlLoader(resources, yamlFile);
			loader.loadTables();
			shutdown();
		}
		if (requiresApp) {
			// Run as --scan or --daemon or --server
			AgentMain.main(cmd, resources);
		}
	}
	
	public static Resources getResources() {
		return resources;
	}
	
	public static void shutdown() {
		Log.shutdown();
	}
	
	public static Thread getThread() {
		assert mainThread != null;
		return mainThread;
	}
	
	public static void interrupt() {
		assert logger != null;
		logger.info(Log.FINISH, "interrupt");
		mainThread.interrupt();
	}
	
	public static void sleep(int millisec) {
		assert logger != null;
		try {
			logger.info(Log.FINISH, String.format("sleep %d", millisec));
			Thread.sleep(millisec);
		} catch (InterruptedException e) {
			logger.warn(Log.ERROR, e.getMessage());
		}		
	}
		
}
