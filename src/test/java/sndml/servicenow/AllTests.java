package sndml.servicenow;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	DateTimeTest.class, 
	ParametersTest.class,
	FieldNamesTest.class,
	InstanceTest.class, 
	SessionIDTest.class,
	SessionVerificationTest.class,
	TableWSDLTest.class,
	TableSchemaTest.class,
	GetKeysTest.class,
	RestTableReaderTest.class,
	SetFieldsTest.class,
	CRUDTest.class,
	})

public class AllTests {

}
