package sndml.datamart;

import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sndml.servicenow.*;

public class JobConfig {

	static final Logger logger = LoggerFactory.getLogger(JobConfig.class);	
	
	private DateTimeFactory dateFactory;
	@JsonIgnore public DateTime start;
	public Key sys_id;
	public String number;
	public String name;
	public String source;
	public String target;
	public Action action;
	public Boolean truncate;
	@JsonProperty("drop") public Boolean dropTable;
	@JsonProperty("created") public JsonNode createdExpr;
	@JsonProperty("since") public String sinceExpr;
	@JsonIgnore public DateTimeRange createdRange;
	@JsonIgnore public DateTime sinceDate;
	public String filter;
	public Interval partition;
	public Integer pageSize;
	public Integer minRows;
	public Integer maxRows;
	public String sqlBefore;
	public String sqlAfter;
	public Boolean autoCreate;
	@JsonIgnore public FieldNames includeColumns;
	@JsonIgnore public FieldNames excludeColumns;
	public Integer threads;

	public JobConfig() {		
	}
	
	public void setDateFactory(DateTimeFactory factory) {
		this.dateFactory = factory;
	}

	@JsonIgnore
	void setCreated(DateTimeRange value) {
		this.createdRange = value;
	}
		
	void setCreated(JsonNode node) {
		assert node != null;
		assert dateFactory != null;
		if (node.isTextual()) {
			String s1 = node.asText();
			DateTime d1 = dateFactory.getDate(s1);
			this.createdRange = new DateTimeRange(d1, null);				
		}
		else if (node.isArray()) {
			int len = node.size();
			String s1 = len > 0 ? node.get(0).asText() : null;
			String s2 = len > 1 ? node.get(1).asText() : null;
			DateTime d1 = dateFactory.getDate(s1);
			DateTime d2 = dateFactory.getDate(s2);
			this.createdRange = new DateTimeRange(d1, d2);				
		}
		else
			throw new ConfigParseException("Invalid created: " + node);		
	}
	
	String getName() { return this.name; }
	String getSource() { return this.source; }
	String getTarget() { return this.target; }
	Key getSysId() { return this.sys_id; }
	String getNumber() { return this.number; }
	Action getAction() { return action; }
	boolean getTruncate() {	return this.truncate == null ? false : this.truncate.booleanValue(); }
	boolean getDropTable() { return this.dropTable == null ? false : this.dropTable.booleanValue(); }
	boolean getAutoCreate() { return this.autoCreate == null ? false : this.autoCreate.booleanValue(); }
	DateTime getSince() { return this.sinceDate; }
	DateTimeRange getCreated() { return this.createdRange; }
	String getFilter() { return this.filter; }
		
	Interval getPartitionInterval() { return this.partition; }
	
	FieldNames getIncludeColumns() { return this.includeColumns; }
	FieldNames getExcludeColumns() { return this.excludeColumns; }
	String getSqlBefore() {	return this.sqlBefore; }
	String getSqlAfter() { return this.sqlAfter; }
	Integer getPageSize() { return this.pageSize; }
	Integer getMinRows() { return this.minRows;	}
	Integer getMaxRows() { return this.maxRows;	}
	Integer getThreads() { return this.threads;	}	

	DateTimeRange getDefaultRange() {
		assert this.start != null;
		return new DateTimeRange(null, this.start);
	}

	void updateFields(DateTimeFactory dateFactory) {
		JobConfig job = this;
		// Determine Start
		if (job.start == null) {
			job.start = dateFactory.getStart();
		}
		// Determine Action
		if (job.action == null)	job.action = 
				Boolean.TRUE.equals(job.truncate) ?	
				Action.LOAD : Action.REFRESH;		
		if (job.action == Action.INSERT) job.action = Action.LOAD;
		if (job.action == Action.UPDATE) job.action = Action.REFRESH;
		
		// Determine Source, Target and Name
		if (job.source == null)	job.source = 
				job.name != null ? job.name : job.target;
		if (job.name == null) job.name = 
				job.target != null ? job.target : job.source;
		if (job.target == null)	job.target = 
				job.source != null ? job.source : job.name;
		
		// AutoCreate defaults to True
		if (job.autoCreate == null) {
			if (job.action==Action.LOAD || job.action==Action.REFRESH || job.action==Action.SYNC)
				job.autoCreate = Boolean.TRUE;			
		}
		
		if (job.sinceExpr == null) {
			job.sinceDate = null;
		}
		else {
			job.sinceDate = dateFactory.getDate(job.sinceExpr);
		}
		
		if (job.createdExpr == null) {
			job.createdRange = null;
		}
		if (job.createdExpr != null) {
			job.setDateFactory(dateFactory);
			job.setCreated(job.createdExpr);
		}

				
		/*
		if (config.pageSize == null && config.parent() != null) {
			config.pageSize = config.parent.getPageSize();
		}
		*/
	}

	void validate() {
		JobConfig job = this;
		if (action == null) configError("Action not specified");
		if (job.getSource() == null) configError("Source not specified");
		if (job.getTarget() == null) configError("Target not specified");
		if (job.getName() == null) configError("Name not specified");
		booleanValidForActions("Truncate", job.truncate, action, 
				EnumSet.of(Action.LOAD));
		booleanValidForActions("Drop", job.dropTable, action, 
				EnumSet.of(Action.CREATE));
		validForActions("Created", job.createdRange, action, 
				EnumSet.range(Action.LOAD, Action.SYNC));
		validForActions("Since", job.sinceDate, action, 
				EnumSet.range(Action.LOAD, Action.REFRESH));
		
		if (job.sinceDate == null && job.sinceExpr != null)
			configError("Missing Since Date");
		if (job.createdRange == null & job.createdExpr != null)
			configError("Missing Created Range");
		
		if (job.getIncludeColumns() != null && job.getExcludeColumns() != null) 
			configError("Cannot specify both Columns and Exclude");		
		
	}
	
	/*
	boolean isValid() {
		String msg = null;		
		if (msg==null && action==null) msg = "Missing action";
		if (msg==null && source==null) msg = "Missing source";
		if (msg==null && target==null) msg = "Missing target";
		if (msg==null && sinceDate==null && sinceValue!=null) 
			msg = "Missing Since Date";
		if (msg==null && createdRange==null && createdValue!=null) 
			msg = "Missing Created Range";
		if (msg != null) logger.warn(Log.INIT, msg);
		return msg==null ? true : false;
	}
	*/
	
	
	static void booleanValidForActions(String name, Boolean value, Action action, EnumSet<Action> validActions) {
		if (Boolean.TRUE.equals(value)) {
			if (!validActions.contains(action))
				notValid(name, action);
		}
	}
	
	static void validForActions(String name, Object value, Action action, EnumSet<Action> validActions)
			throws ConfigParseException {
		if (value != null) {
			if (!validActions.contains(action))
				notValid(name, action);
		}		
	}
		
	static void notValid(String option, Action action) throws ConfigParseException {
		String msg = option + " not valid with Action: " + action.toString();
		configError(msg);
	}

	static void configError(String msg) {
		throw new ConfigParseException(msg);
	}	
		
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		if (sys_id != null) node.put("sys_id",  sys_id.toString());
		if (number != null) node.put("number",  getNumber());
		node.put("name", getName());
		node.put("source", getSource());
		node.put("target", getTarget());
		node.put("action", action.toString());
		if (getTruncate()) node.put("truncate", true);
		if (getDropTable()) node.put("drop", true);
		if (getAutoCreate()) node.put("autocreate", getAutoCreate());
		if (getSince() != null) node.put("since", getSince().toString());
		if (getCreated() != null) node.set("created", getCreated().toJsonNode());
		if (getPartitionInterval() != null) node.put("partition",  getPartitionInterval().toString());
		if (getFilter() != null) node.put("filter", getFilter());
		String yaml;
		try {
			yaml = mapper.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		return yaml;
	}
		
}
