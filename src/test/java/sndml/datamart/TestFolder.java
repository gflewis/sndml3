package sndml.datamart;

import java.io.File;
import java.io.FilenameFilter;

/**
 * A folder in src/test/resources which contains files for JUnit tests.
 *
 */
@SuppressWarnings("serial")
public class TestFolder extends File {

	FilenameFilter yamlFilter = (dir, name) -> name.endsWith(".yaml");			
	
	@SuppressWarnings("rawtypes")
	public TestFolder(Class myclass) {
		this(myclass.getSimpleName());
	}
	
	public TestFolder(String name) {
		super("src/test/resources/YAML", name);
	}
		
	/**
	 * Get a file from this folder
	 */
	public File getFile(String filename) {
		return new File(this, filename);
	}
	
	public YamlFile getYaml(String name) {
		return new YamlFile(new File(this, name + ".yaml"));
	}
	
	public YamlFile[] yamlFiles() {
		File[] files = listFiles(yamlFilter);
		YamlFile[] result = new YamlFile[files.length];
		for (int i = 0; i < files.length; ++i) result[i] = new YamlFile(files[i]);
		return result;
	}

}
