package servicenow.api;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import servicenow.datamart.Globals;
import servicenow.datamart.ResourceManager;

public class TestingManager {
	
	static final Logger logger = LoggerFactory.getLogger(TestingManager.class);
	
	static final String TEST_PROPERTIES = "junit.properties";
	
	static Session defaultSession;
	static Properties testProperties = getTestProperties();
	static TestingProfile currentProfile;
	@SuppressWarnings("rawtypes")
	static Class classUnderTest;

	/**
	 * Get a profile from profiles/name/.sndml_profile.
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
		ResourceManager.setSession(profile.getSession());
		ResourceManager.setDatabase(profile.getDatabase());
		return profile;
	}
	
	@Deprecated
	public static TestingProfile setProfile(TestingProfile profile) {
		currentProfile = profile;
		return profile;		
	}
	
	public static TestingProfile setDefaultProfile() {
		return setProfile(getDefaultProfile());
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

	@Deprecated
	public static Session getSession() throws TestingException {
		return currentProfile.getSession();
	}
	
	@Deprecated
	public static Session getDefaultSession() throws TestingException {
		return getDefaultProfile().getSession();
	}
	
	@Deprecated
	public static void loadProfile(TestingProfile profile, boolean showBanner) throws TestingException {
		// currentProfile = profile;
		if (showBanner) 
			Log.banner(logger, Log.INIT, "loadProfile " + profile.getName());
		else
			logger.info(Log.INIT, "loadProfile " + profile.getName());
		ResourceManager.initialize(profile.getProperties());
		Globals.setStart(DateTime.now());
	}
	
	@Deprecated
	public static void loadProfile(TestingProfile profile) throws TestingException {
		loadProfile(profile, false);
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
	static public File yamlFile(String name) {
		assert name != null;
		return new File("src/test/resources/yaml/" + name + ".yaml");
	}
	
	public static void banner(Logger logger, String msg) {
		Log.banner(logger, Log.TEST, msg);
	}

	@SuppressWarnings("rawtypes")
	public static void bannerStart(Class cls, TestingProfile profile, String testName) {
		Logger logger = getLogger(cls);
		String msg = "Begin: " + testName + " Profile: " + profile.getName();
		banner(logger, msg);		
	}

	@SuppressWarnings("rawtypes")
	public static void bannerStart(Class cls, String testName) {
		Logger logger = getLogger(cls);
		String msg = "Begin: " + testName + " Profile: " + currentProfile.getName();
		banner(logger, msg);		
	}
	
	public static void bannerStart(String testName) {
		assert classUnderTest != null;
		assert currentProfile != null;
		bannerStart(classUnderTest, currentProfile, testName);
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
