package servicenow.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.YamlFile;
import sndml.servicenow.FieldNames;
import sndml.servicenow.Log;
import sndml.servicenow.Session;

public class TestingManager {
	
	static final Logger logger = LoggerFactory.getLogger(TestingManager.class);
	
	static final String TEST_PROPERTIES = "junit.properties";
	
	static Session defaultSession;
	static Properties testProperties = getTestProperties();
	static TestingProfile currentProfile;
	@SuppressWarnings("rawtypes")
	static Class classUnderTest;

	/**
	 * Get a profile from configs/name/.sndml_profile.
	 * Note that the profiles directory is NOT not stored in github it they may contain passwords.
	 */
	public static TestingProfile getProfile(String name) {
		try {
			return new TestingProfile(name);
		} catch (IOException e) {
			throw new TestingException(e);
		}
	}

	public static TestingProfile getDefaultProfile() throws TestingException {
		return getProfile(getProperty("api.default_profile"));
	}
	
	@SuppressWarnings("rawtypes")
	public static TestingProfile setProfile(Class myclass, TestingProfile profile) {
		classUnderTest = myclass;
		currentProfile = profile;
		// ResourceManager.setSession(profile.getSession());
		// ResourceManager.setDatabase(profile.getDatabase());
		return profile;
	}
		
	@SuppressWarnings("rawtypes")
	public static TestingProfile setDefaultProfile(Class myclass) {
		return setProfile(myclass, getDefaultProfile());
	}
	
	public static TestingProfile getProfile() {
		return currentProfile;
	}
	
	public static void clearAll() {
		if (currentProfile != null) {
			currentProfile.close();
		}
		currentProfile = null;
		classUnderTest = null;
	}
	
	public static TestingProfile[] getProfiles(String names) {
		FieldNames list = new FieldNames(names);
		TestingProfile[] profiles = new TestingProfile[list.size()];
		for (int i=0; i<list.size(); ++i) {
			profiles[i] = getProfile(list.get(i));
		}
		return profiles;		
	}
	
	public static TestingProfile[] getDatamartProfiles() {
		return getProfiles(getProperty("datamart.profile_list"));
	}
	
	/**
	 * Returns an array of all available database profiles.
	 * Parameterized tests will execute for each of these profiles.
	 * @return
	 */
	public static TestingProfile[] allProfiles() {
		return getProfiles(getProperty("datamart.profile_list"));
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

	@Deprecated
	public static Properties getProperties() throws TestingException {
		return testProperties;
	}
		
	/**
	 * Return a value used for testing.
	 * These properties are found in the file test/resources/junit.properties.
	 * All properties have a prefix of junit.
	 * 
	 * @param name Name of property with "junit." prefix omitted
	 * @return Property value
	 * @throws TestingException
	 */
	public static String getProperty(String name) throws TestingException {
		logger.info(Log.INIT, "getProperty " + name);
		String propname = "junit." + name;
		String value = testProperties.getProperty(propname);
		if (value == null) throw new IllegalArgumentException(propname);
		return value;
	}

	/**
	 * Returns a File in the src/test/resources/yaml directory.
	 * These files are used for YAML unit tests.
	 */
		
	public static void banner(Logger logger, String msg) {
		Log.banner(logger, Log.TEST, msg);
	}

	@SuppressWarnings("rawtypes")
	public static void bannerStart(Class cls, String testName, 
			TestingProfile profile, YamlFile config) {
		Logger logger = getLogger(cls);
		StringBuilder msg = new StringBuilder("Begin:" + testName);
		if (profile != null) msg.append(" Profile:" + profile.getName());
		if (config != null) msg.append(" Config:" + config.getName());
		banner(logger, msg.toString());		
	}

	@SuppressWarnings("rawtypes")
	public static void bannerStart(Class cls, String testName) {
		bannerStart(cls, testName, null, null);
	}
	
	public static void bannerStart(String testName) {
		assert classUnderTest != null;
		assert currentProfile != null;
		bannerStart(classUnderTest, testName, currentProfile, null);
	}
	
	@SuppressWarnings("rawtypes")	
	public static void bannerStart(Class cls, String testName, YamlFile config) {
		bannerStart(cls, testName, null, config);
	}
	
	@Deprecated
	public static void banner(Logger logger, String classname, String msg) {
		Log.banner(logger, Log.TEST, classname + " - " + msg);
	}
	
	@Deprecated
	public static void banner(Logger logger, @SuppressWarnings("rawtypes") Class cls, String msg) {
		banner(logger, cls.getSimpleName(), msg);
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
