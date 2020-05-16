package servicenow.datamart;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.api.Log;
import servicenow.api.Session;

public class ConnectionProfile {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final Properties properties = new Properties();
	private final File file;
	private Session session;
	private Database database;
	
	public ConnectionProfile(File path) throws IOException {
		file = path;
		logger.info(Log.INIT, "Load profile " + file.getAbsolutePath());
		FileInputStream stream = new FileInputStream(file);
		assert stream != null;
		Properties raw = new Properties();
		raw.load(stream);
		Pattern cmdPattern = Pattern.compile("^`(.+)`$");
		for (String name : raw.stringPropertyNames()) {
			String value = raw.getProperty(name);
			// If property is in backticks then evaluate as a command 
			Matcher cmdMatcher = cmdPattern.matcher(value); 
			if (cmdMatcher.matches()) {
				logger.info(Log.INIT, "evaluate " + name);
				String command = cmdMatcher.group(1);
				value = Globals.evaluate(command);
				if (value == null || value.length() == 0)
					throw new AssertionError(String.format("Failed to evaluate \"%s\"", command));
				logger.debug(Log.INIT, value);
			}
			properties.setProperty(name, value);			
		}
	}

	public File getFile() {
		return this.file;
	}
	
	public Properties getProperties() {
		return this.properties;
	}
	
	public Session getSession()  {
		if (session == null) {
			session = new Session(properties);
		}
		return session;
	}
	
	public Database getDatabase() throws ResourceException {
		if (database == null) {
			try {
				database = new Database(properties);
			} catch (SQLException | URISyntaxException e) {
				throw new ResourceException(e);
			}
		}
		return database;
	}
	
	public void close() {
		logger.info(Log.FINISH, "Close profile " + file.getAbsolutePath());
		try {
			if (database != null) database.close();
			if (session != null) session.close();
		} catch (IOException | SQLException e) {
			logger.error(Log.FINISH, "ConnectionProfile.close() failure");;
		}
		database = null;
		session = null;
	}
	
	@Override
	public String toString() {
		return file.getAbsolutePath();
	}
		
}
