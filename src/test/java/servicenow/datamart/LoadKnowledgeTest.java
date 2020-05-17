package servicenow.datamart;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Test;

import servicenow.api.TestingManager;
import servicenow.api.TestingProfile;
import servicenow.api.WriterMetrics;

public class LoadKnowledgeTest {

	TestingProfile profile;
	
	public LoadKnowledgeTest() {
		profile = TestingManager.getDefaultProfile();
		TestingManager.setProfile(this.getClass(), profile);
	}

	@AfterClass
	public static void clear() throws Exception {
		TestingManager.clearAll();
	}			
	
	@Test
	public void test() {
		TestLoader loader1 = new TestLoader(new YamlFile("load_kb_knowledge"));
		WriterMetrics metrics1 = loader1.load();
		int processed = metrics1.getProcessed();
		assertTrue(processed > 0);
	}

}
