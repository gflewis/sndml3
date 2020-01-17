package servicenow.datamart;

import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Utility class to help parse YAML configuration files
 */
public class Config {

	static Yaml parser = new Yaml();
	static Logger logger = LoggerFactory.getLogger(Config.class);
	
	public class Map extends java.util.LinkedHashMap<String, Object> {
		private static final long serialVersionUID = 1L;	

		@SuppressWarnings("unchecked")
		public Map(Object obj) throws ConfigParseException {
			try {
				for (java.util.Map.Entry<Object,Object> entry : ((java.util.Map<Object,Object>) obj).entrySet()) {
					this.put(entry.getKey().toString(), entry.getValue());				
				}
			}
			catch (ClassCastException e) {
				throw new ConfigParseException(e);
			}
			
		}

		public Config.List getList(String name) throws ConfigParseException {
			return new List(get(name));
		}

		/**
		 * @return String value if defined otherwise null;
		 */
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
	
	public Config.List toList(Object obj) throws ConfigParseException {
		return new Config.List(obj);
	}
	
	public Map parseDocument(Reader reader) throws ConfigParseException {
		try {
			return new Map(parser.load(reader));
		}
		catch (Exception e) {
			throw new ConfigParseException(e);
		}
		
	}
	
	public static boolean isMap(Object obj) {
		return obj instanceof java.util.Map;
	}
	
	public Config.Map toMap(Object obj) throws ConfigParseException {
		return new Config.Map(obj);
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
			
}
