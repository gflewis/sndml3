package servicenow.api;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	DateTimeTest.class, 
	InstanceTest.class, 
	ParametersTest.class,
	SessionIDTest.class,
	TableSchemaTest.class,
	RestTableReaderTest.class,
	})
public class AllTests {

}
