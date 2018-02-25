package servicenow.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.datamart.ResourceManager;

public class TestingManager {

	static final Logger logger = LoggerFactory.getLogger(TestingManager.class);
	
	static final String TEST_PROFILES_DIR = "profiles/";
	static final String DEFAULT_PROFILE = "mydev";
	static final String TEST_PROPERTIES = "junit.properties";
	
	static Properties properties = getTestProperties();

	/**
	 * Load a unit test profile, which is a properties file with a ".profile" extension.
	 * Test profiles are not stored in github because they may contain passwords.
	 * This function will throw an exception if the profiles directory
	 * has not been initialized.
	 */
	public static void loadProfile(String name) throws TestingException {
		logger.info(Log.INIT, "loadProfile " + name);
		String filename = TEST_PROFILES_DIR + name + ".profile";
		try {
			FileInputStream stream = new FileInputStream(filename);
			assert stream != null;
			properties.load(stream);
		}
		catch (IOException e) {
			throw new TestingException("Unable to load testing profile: " + name, e);
		}
		ResourceManager.initialize(properties);
	}
	
	public static void loadDefaultProfile() throws TestingException {
		loadProfile(DEFAULT_PROFILE);
	}

	@SuppressWarnings("rawtypes")
	public static Logger getLogger(Class cls) {
		return LoggerFactory.getLogger(cls);
	}

	public static Session getSession() throws TestingException {
		Properties props = getProperties();
		String instanceName = props.getProperty("servicenow.instance");
		if (instanceName == null) 
			throw new TestingException("Profile not loaded");
		Session session =  new Session(props);
		return session;
	}
	
	private static Properties getTestProperties() {
		String propFileName = TEST_PROPERTIES;
		Properties props = new Properties();
		logger.info(Log.INIT, "loadProperties " + propFileName);
		try {
			InputStream stream = ClassLoader.getSystemResourceAsStream(propFileName);
			props.load(stream);
		} catch (IOException e) {
			logger.error("Unable to load: " +propFileName);
			System.exit(-1);
		}
		return props;
	}
	
	public static Properties getDefaultProperties() throws TestingException {
		loadDefaultProfile();
		return properties;		
	}
	
	public static Properties getProperties() throws TestingException {
		return properties;
	}

	public static String getProperty(String name) throws TestingException {
		logger.info(Log.INIT, "getProperty " + name);
		String propname = "junit." + name;
		String value = getProperties().getProperty(propname);
		if (value == null) throw new IllegalArgumentException(propname);
		return value;
	}

	public static void banner(Logger logger, String msg) {
		int len = msg.length();
		String bar = "\n" + StringUtils.repeat("*", len + 4);
		logger.info(Log.TEST, DateTime.now() + " " + msg + bar + "\n* " + msg + " *" + bar);
	}

	public static void banner(Logger logger, String classname, String msg) {
		banner(logger, classname + " - " + msg);
	}
	
	public static void banner(Logger logger, @SuppressWarnings("rawtypes") Class klass, String msg) {
		banner(logger, klass.getSimpleName(), msg);
	}
	
	
	public static void sleep(int sleepSeconds) throws InterruptedException {
		String msg = "Sleeping " + sleepSeconds + " sec...";
		logger.info(Log.TEST, msg);
		Thread.sleep(1000 * sleepSeconds);
	}
	
	
}
