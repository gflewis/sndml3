package sndml.datamart;

import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Utility class to help parse YAML configuration files
 */
@Deprecated
public class YamlConfig {

	static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
	
	// static Yaml yamlParser = new Yaml();
	static Logger logger = LoggerFactory.getLogger(YamlConfig.class);
	
	/*
	public class Map extends java.util.LinkedHashMap<String, Object> {
		private static final long serialVersionUID = 1L;	

		public Map(JSONObject obj) throws ConfigParseException {
			java.util.Set<java.util.Map.Entry<String,Object>> entrySet = obj.entrySet();
			
		}

		public Map(java.util.Map<String,Object> obj) throws ConfigParseException {
			try {
				for (java.util.Map.Entry<String,Object> entry : obj.entrySet()) {
					this.put(entry.getKey().toString(), entry.getValue());				
				}
			}
			catch (ClassCastException e) {
				throw new ConfigParseException(e);
			}
			
		}

		public YamlConfig.List getList(String name) throws ConfigParseException {
			return new List(get(name));
		}

		public String getString(String name) throws ConfigParseException {
			return getString(name, null);
		}
		
		public String getString(String name, String defaultValue) throws ConfigParseException {
			return containsKey(name) ? get(name).toString() : defaultValue;			
		}
		
		Integer getInteger(String name) throws ConfigParseException {
			return getInteger(name, null);
		}
		
		public Integer getInteger(String name, Integer defaultValue) throws ConfigParseException {
			return containsKey(name) ? asInteger(this.get(name)) : defaultValue;
		}
	}
	
	public class List extends java.util.ArrayList<Object> {
		private static final long serialVersionUID = 1L;
		
		@SuppressWarnings("unchecked")
		public List(Object obj) throws ConfigParseException {
			try {
				for (Object item : (java.util.List<Object>) obj) {
					this.add(item);
				}
			}
			catch (ClassCastException e) {
				throw new ConfigParseException(e);
			}
			
		}
	}

	public static boolean isList(Object obj) {
		return obj instanceof java.util.List;
	}
	
	public YamlConfig.List toList(Object obj) throws ConfigParseException {
		return new YamlConfig.List(obj);
	}
	*/
	
	public ObjectNode parseYAML(Reader reader) throws ConfigParseException {
		try {
			JsonNode root = yamlMapper.readTree(reader);
			return (ObjectNode) root;
		}
		catch (Exception e) {
			throw new ConfigParseException(e);
		}		
	}
	
	/*
	public static boolean isMap(Object obj) {
		return obj instanceof java.util.Map;
	}

	@SuppressWarnings("unchecked")
	public YamlConfig.Map toMap(Object obj) throws ConfigParseException { 
		return new YamlConfig.Map((java.util.Map<String,Object>) obj);
	}
		
	public static Integer asInteger(Object obj) throws ConfigParseException {
		Integer result;
		try {
			result = (Integer) obj;
		}
		catch (ClassCastException e) {
			throw new ConfigParseException("Invalid integer: " + obj.toString(), e);
		}
		return result;
	}
	
	public static Boolean asBoolean(Object obj) throws ConfigParseException {
		Boolean result;
		try {
			result = (Boolean) obj;
		}
		catch (ClassCastException e) {
			throw new ConfigParseException("Invalid boolean: " + obj.toString(), e);
		}
		return result;
	}
	*/
			
}
