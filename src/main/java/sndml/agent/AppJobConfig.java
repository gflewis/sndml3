package sndml.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.loader.ConfigParseException;
import sndml.loader.JobConfig;
import sndml.servicenow.RecordKey;

public class AppJobConfig extends JobConfig {

	// TODO Rename sys_id to runkey. sys_id is ambiguous.
	public RecordKey sys_id;
	public String number;
	
	static final Logger logger = LoggerFactory.getLogger(AppJobConfig.class);	
	
	public AppJobConfig() {
		super();
	}

	@Override
	protected void updateCoreFields() {
		super.updateCoreFields();
		if (jobName == null && number != null) jobName = number;		
	}
		
	@Override	
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		if (sys_id != null) node.put("sys_id",  sys_id.toString());
		if (number != null) node.put("number",  getNumber());
		addFieldsToObject(node);
		String yaml;
		try {
			yaml = mapper.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		return yaml;
	}

	public RecordKey getSysId() {
		assert this.sys_id != null;
		return this.sys_id; 
	}	
	
	public String getNumber() {
		assert this.number != null;
		return this.number; 
	}
	
	@Override
	public String getName() { 
		return getNumber(); 
	}
	
}
