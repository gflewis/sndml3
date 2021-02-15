package sndml.datamart;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	EvaluateTest.class,
	ConfigParseExceptionTest.class,
	LoaderConfigTest.class,
	DateTimeFactoryTest.class,
	CreateTableTest.class,
	InsertTest.class,
	PruneTest.class,
	TimestampTest.class,
	TableLoaderTest.class,
})

public class AllTests {
		
}
