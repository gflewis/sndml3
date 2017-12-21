package servicenow.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import servicenow.core.Log;
import servicenow.core.Session;

public class TestingManager {

	static final Logger logger = LoggerFactory.getLogger(TestingManager.class);
	public static final Marker mrkTest = MarkerFactory.getMarker("TEST");
	
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
	
}
