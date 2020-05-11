package servicenow.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.datamart.Globals;
import servicenow.datamart.ResourceManager;

public class TestingManager {

	static final Logger logger = LoggerFactory.getLogger(TestingManager.class);
	
	static final String TEST_PROPERTIES = "junit.properties";
	
	static Properties properties = getTestProperties();
	static Session defaultSession;
	static String currentProfile;

	/**
	 * Load a connection profile from profiles/name/.sndml_profile.
	 * Note that the profiles directory is NOT not stored in github it they may contain passwords.
	 * This function will throw an exception if the profiles directory has not been initialized.
	 */
	public static void loadProfile(String name, boolean showBanner) throws TestingException {
		currentProfile = name;
		if (showBanner) 
			Log.banner(logger, Log.INIT, "loadProfile " + name);
		else
			logger.info(Log.INIT, "loadProfile " + name);
		Path directory = Paths.get("profiles",  name);
		File profile = directory.resolve(".sndml_profile").toFile();
		try {
			FileInputStream stream = new FileInputStream(profile);
			assert stream != null;
			properties.load(stream);
		}
		catch (IOException e) {
			throw new TestingException("Unable to load testing profile: " + name, e);
		}
		ResourceManager.initialize(properties);
		Globals.setStart(DateTime.now());
	}
	
	public static void loadProfile(String name) throws TestingException {
		loadProfile(name, false);
	}
	
	public static void loadDefaultProfile() throws TestingException {
		String defaultProfile = getProperty("api.default_profile");
		loadProfile(defaultProfile);
	}

	static Hashtable<String,Logger> myLoggers = new Hashtable<String,Logger>();
	
	@SuppressWarnings("rawtypes")
	public static Logger getLogger(Class cls) {
		String className = cls.getName();
		if (myLoggers.contains(className)) return myLoggers.get(className);
		Logger logger = LoggerFactory.getLogger(className);
		myLoggers.put(className, logger);
		return logger;
	}

	public static Session getSession() throws TestingException {
		Properties props = getProperties();
		String instanceName = props.getProperty("servicenow.instance");
		if (instanceName == null) 
			throw new TestingException("Profile not loaded");
		Session session =  new Session(props);
		return session;
	}
	
	public static Session getDefaultSession() throws TestingException {
		if (defaultSession == null) {
			loadDefaultProfile();
			defaultSession = getSession();
		}
		return defaultSession;
	}

	/**
	 * Returns a string array of all available database profiles.
	 * Parameterized tests will execute for each of these profiles.
	 * @return
	 */
	public static String[] allProfiles() {
		FieldNames allProfiles = new FieldNames(getProperty("datamart.profile_list"));
		return allProfiles.toArray();
	}
	
	/**
	 * This function is called during TestingManager initialization.
	 * If it fails, then it is bad news. (Note the System.exit(-1).)
	 */
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

	/**
	 * Return a value used for testing.
	 * These properties are found in the file test/resources/junit.properties.
	 * All properties have a prefix of junit.
	 */
	public static String getProperty(String name) throws TestingException {
		logger.info(Log.INIT, "getProperty " + name);
		String propname = "junit." + name;
		String value = getProperties().getProperty(propname);
		if (value == null) throw new IllegalArgumentException(propname);
		return value;
	}

	/**
	 * Returns a File in the src/test/resources/yaml directory.
	 * These files are used for YAML unit tests.
	 */
	static public File yamlFile(String name) {
		assert name != null;
		return new File("src/test/resources/yaml/" + name + ".yaml");
	}
	
	public static void banner(Logger logger, String msg) {
		Log.banner(logger, Log.TEST, msg);
	}

	@SuppressWarnings("rawtypes")
	public static void bannerStart(Class cls, String testName) {
		Logger logger = getLogger(cls);
		String profile = currentProfile;
		String msg = "Begin: " + testName + " Profile: " + profile;
		banner(logger, msg);		
	}
	
	@Deprecated
	public static void banner(Logger logger, String classname, String msg) {
		Log.banner(logger, Log.TEST, classname + " - " + msg);
	}
	
	@Deprecated
	public static void banner(Logger logger, @SuppressWarnings("rawtypes") Class klass, String msg) {
		banner(logger, klass.getSimpleName(), msg);
	}
		
	public static String randomName(int length) {
		final String lexicon = "abcdefghijklmnopqrstuvwxyz";
		final java.util.Random rand = new java.util.Random();		
	    StringBuilder builder = new StringBuilder();
	    while(builder.toString().length() == 0) {
	        for(int i = 0; i < length; i++) {
	            builder.append(lexicon.charAt(rand.nextInt(lexicon.length())));
	        }
	    }
	    return builder.toString();		
	}
	
	public static String randomName() {
		return randomName(10);
	}
	
	public static void sleep(int sleepSeconds) throws InterruptedException {
		String msg = "Sleeping " + sleepSeconds + " sec...";
		logger.info(Log.TEST, msg);
		Thread.sleep(1000 * sleepSeconds);
	}
	
	
}
