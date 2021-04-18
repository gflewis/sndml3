package sndml.datamart;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	EvaluateTest.class,
	YamlParseValidTest.class,
	YamlParseErrorTest.class,
	LoaderConfigTest.class,
	DateTimeFactoryTest.class,
	CreateTableTest.class,
	InsertTest.class,
	RefreshTest1.class,
	RefreshTest2.class,
	RowCountExceptionTest.class,
	PruneTest.class,
	TimestampTest.class,
	TableLoaderTest.class,
})

public class AllTests {
		
}
