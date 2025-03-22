package sndml.loader;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
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
import sndml.util.ResourceException;

/**
 * This is the main class invoked from the JAR file. This is a singleton class.
 */
public class Main {

	static protected final Thread mainThread = Thread.currentThread();		
	static protected ConnectionProfile profile;
	static protected Resources resources;
	static private boolean requiresApp = false;
	static private CommandLine cmd;
	static protected String agentName;
	static protected long pid;

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

	static final Option commandOptions[] = {
			optYaml, optTable, optJobRun, optScan, optDaemon, optServer};
	
	static private Logger logger;

	public static void main(String[] args) throws Exception {
		
		final Option allOptions[] = {
			optProfile, optTable, optFilter, optSysID, optYaml, 
			optJobRun, optScan, optDaemon, optServer};		

		for (Option opt : allOptions) {
			options.addOption(opt);
		}
						
		cmd = new DefaultParser().parse(options,  args);
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
		Main.writePidfile();

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

	private static void writePidfile() throws ResourceException {
        ProcessHandle processHandle = ProcessHandle.current();
        String pidFileName = profile.getProperty("loader.pidfile");
		Main.pid = processHandle.pid();
		if (pidFileName == null) {
			logger.info(Log.INIT, String.format("pid=%d", pid));			
		}
		else {
			File pidFile = new File(pidFileName);
			logger.info(Log.INIT, String.format(
				"pid=%d pidfile=%s", pid, pidFile.getAbsolutePath()));
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
	
	protected static Option getCommandOption() {
		for (Option opt : commandOptions) {
			if (cmd.hasOption(opt)) return opt;			
		}
		return null;		
	}
	
	protected static String getCommandOptionName() {
		return getCommandOption().getLongOpt();		
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
	
}
