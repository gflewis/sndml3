package servicenow.datamart;

import servicenow.api.*;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@RunWith(Suite.class)
@SuiteClasses({ 
	DateTimeTest.class, 
	InstanceTest.class, 
	ParametersTest.class })

public class AllTests {

//	public static final Logger logger = LoggerFactory.getLogger(AllTests.class);
//	public static final Marker mrkTest = MarkerFactory.getMarker("TEST");
	
//	static Database dbw = null;
//	static Properties properties = null;
//		
//	@Deprecated
//	public static Database getDBWriter() throws IOException, java.sql.SQLException {
//		try {
//			dbw = new Database(TestingManager.getProperties());
//		} catch (URISyntaxException e) {
//			throw new TestingException(e);
//		}
//		return dbw;
//	}

//	@SuppressWarnings("rawtypes")
//	public static Logger getLogger(Class cls) {
//		return servicenow.api.TestingManager.getLogger(cls);
//	}

	
}
