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

	/**
	 * Returns a File in the src/test/resources/yaml directory.
	 * These files are used for YAML unit tests.
	 */
	@Deprecated
	static public File yamlFile(String name) {
		assert name != null;
		return new File("src/test/resources/yaml/" + name + ".yaml");
	}

	/**
	 * Returns a list of all available database profiles.
	 * Parameterized tests will execute for each of these profiles.
	 * @return
	 */
	@Deprecated
	public static String[] allProfiles() {
		// return new String[] {"awsmysql","awsmssql", "awspg", "awsora"};
		return new String[] {"mydev"};
	}
		
}
