package sndml.servicenow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import sndml.datamart.ConfigParseException;
import sndml.datamart.YamlFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class TestManager {
		
	static TestManager manager = null;
	static final String TEST_PROPERTIES = "junit.properties";
	
	Session defaultSession;
	Properties testProperties;
	TestingProfile currentProfile;
	ObjectMapper jsonMapper = new ObjectMapper();
	ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());	
	Logger logger;
	
	@SuppressWarnings("rawtypes")
	Class classUnderTest;

	TestManager() {
		logger = LoggerFactory.getLogger(this.getClass());
		String propFileName = TEST_PROPERTIES;
		testProperties = new Properties();
		logger.info(Log.INIT, "loadProperties " + propFileName);
		try {
			InputStream stream = ClassLoader.getSystemResourceAsStream(propFileName);
			testProperties.load(stream);
		} catch (IOException e) {
			logger.error("Unable to load: " +propFileName);
			System.exit(-1);
		}		
	}
	
	private static void _initialize() {
		if (manager == null) manager = new TestManager();		
	}
	
	/**
	 * Get a profile from configs/name/.sndml_profile.
	 * Note that the profiles directory is NOT not stored in github it they may contain passwords.
	 */
	public static TestingProfile getProfile(String name) {
		_initialize();
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
		_initialize();
		manager.classUnderTest = myclass;
		manager.currentProfile = profile;
		// ResourceManager.setSession(profile.getSession());
		// ResourceManager.setDatabase(profile.getDatabase());
		return profile;
	}
		
	@SuppressWarnings("rawtypes")
	public static TestingProfile setDefaultProfile(Class myclass) {
		return setProfile(myclass, getDefaultProfile());
	}
	
	public static TestingProfile getProfile() {
		_initialize();
		return manager.currentProfile;
	}
	
	public static void clearAll() {
		_initialize();
		manager.currentProfile = null;
		manager.classUnderTest = null;
		manager.defaultSession = null;
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
		assert cls != null;
		String className = cls.getName();
		if (myLoggers.contains(className)) return myLoggers.get(className);
		Logger logger = LoggerFactory.getLogger(className);
		myLoggers.put(className, logger);
		return logger;
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
		_initialize();
		manager.logger.info(Log.INIT, "getProperty " + name);
		String propname = "junit." + name;
		String value = manager.testProperties.getProperty(propname);
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
		assert cls != null;
		_initialize();
		Logger logger = getLogger(cls);
		StringBuilder msg = new StringBuilder("Begin:" + testName);
		if (profile != null) msg.append(" Profile:" + profile.getName());
		if (config != null) msg.append(" Config:" + config.getName());
		banner(logger, msg.toString());		
	}

	@SuppressWarnings("rawtypes")
	public static void bannerStart(Class cls, String testName) {
		_initialize();
		bannerStart(cls, testName, null, null);
	}
	
	public static void bannerStart(String testName) {
		assert manager.classUnderTest != null;
		_initialize();
		bannerStart(manager.classUnderTest, testName, manager.currentProfile, null);
	}
	
	@SuppressWarnings("rawtypes")	
	public static void bannerStart(Class cls, String testName, YamlFile config) {
		_initialize();
		bannerStart(cls, testName, null, config);
	}
	
	@Deprecated
	public static ObjectNode json(String text) throws ConfigParseException {
		JsonNode node;
		try {
			node = manager.jsonMapper.readTree(text);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		assert node.isObject();
		return (ObjectNode) node;
	}
	
	@Deprecated
	public static ObjectNode yaml(String text) throws ConfigParseException {		
		JsonNode node;
		try {
			node = (ObjectNode) manager.yamlMapper.readTree(text);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		assert node.isObject();
		return (ObjectNode) node;
	}
	
	public static String readFully(File file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringBuffer text = new StringBuffer();
		while (reader.ready()) text.append(reader.readLine() + "\n");
		reader.close();
		return text.toString();		
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
	
	public static void sleep(double seconds) throws InterruptedException {
		_initialize();
		assert seconds < 60;
		long millisec = Math.round(1000 * seconds);
		String msg = "Sleeping " + seconds + " sec...";
		manager.logger.info(Log.TEST, msg);
		Thread.sleep(millisec);
	}
	
	
}
