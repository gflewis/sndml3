package sndml.util;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	DateTimeTest.class, 
	MetricsTest.class, 
	ParametersTest.class,
	DatePartitionsTest.class })

public class AllTests {
	
}
