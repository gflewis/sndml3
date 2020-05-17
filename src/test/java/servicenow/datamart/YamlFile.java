package servicenow.datamart;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import servicenow.api.TestingException;

@SuppressWarnings("serial")
public class YamlFile extends File {
	
	public YamlFile(String name) {
		super(String.format("src/test/resources/yaml/%s.yaml", name));
	}
	
	FileReader getReader() {
		try {
			return new FileReader(this);
		} catch (FileNotFoundException e) {
			throw new TestingException(e);
		}
	}
	
}
