package servicenow.datamart;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	LoaderConfigTest.class,
	DateTimeFactoryTest.class,
	CreateTableTest.class,
	PruneTest.class,
})

public class AllTests {
	
}
