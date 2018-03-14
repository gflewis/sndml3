package servicenow.datamart;

import java.io.File;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	LoaderConfigTest.class,
	DateTimeFactoryTest.class,
	DBTest.class,
	CreateTableTest.class,
	InsertTest.class,
	PruneTest.class,
	TimestampTest.class,
})

public class AllTests {

	static public File yamlFile(String name) {
		assert name != null;
		return new File("src/test/resources/yaml/" + name + ".yaml");
	}
		
}
