package sndml.datamart;

import java.io.IOException;
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
	@JsonProperty("name") public String jobName;
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
	public String sql; // Action EXECUTE only
	public String sqlBefore;
	public String sqlAfter;
	public Boolean autoCreate;
	@JsonIgnore public FieldNames includeColumns;
	@JsonIgnore public FieldNames excludeColumns;
	public Integer threads;
	
	static EnumSet<Action> anyLoadAction =
			EnumSet.of(Action.INSERT, Action.UPDATE, Action.SYNC);
	
	String getName() { return this.jobName; }
	String getSource() { return this.source; }
	String getTarget() { return this.target; }
	Key getSysId() { return this.sys_id; }
	String getNumber() { return this.number; }
	Action getAction() { return action; }
	boolean getTruncate() {	return this.truncate == null ? false : this.truncate.booleanValue(); }
	boolean getDropTable() { return this.dropTable == null ? false : this.dropTable.booleanValue(); }
	DateTime getSince() { return this.sinceDate; }
	DateTimeRange getCreated() { return this.createdRange; }
	String getFilter() { return this.filter; }
		
	Interval getPartitionInterval() { return this.partition; }
	
	FieldNames getIncludeColumns() { return this.includeColumns; }
	FieldNames getExcludeColumns() { return this.excludeColumns; }	
	String getSql() { return this.sql; }
	String getSqlBefore() {	return this.sqlBefore; }
	String getSqlAfter() { return this.sqlAfter; }
	Integer getPageSize() { return this.pageSize; }
	Integer getMinRows() { return this.minRows;	}
	Integer getMaxRows() { return this.maxRows;	}
	Integer getThreads() { return this.threads;	}	

	boolean getAutoCreate() { 
		return this.autoCreate == null ? true : this.autoCreate.booleanValue();	
	}
	
	FieldNames getColumns(Table table) throws IOException, InterruptedException {
		if (getIncludeColumns() != null)
			return getIncludeColumns();
		else if (getExcludeColumns() != null)
			return table.getSchema().getFieldsMinus(getExcludeColumns());
		else
			return null;
	}
		
	DateTimeRange getDefaultRange() {
		assert this.start != null;
		return new DateTimeRange(null, this.start);
	}

	void updateFields(ConnectionProfile profile, DateTimeFactory dateFactory) {
		updateCoreFields();
		if (dateFactory != null) updateDateFields(dateFactory);
		if (profile != null) updateFromProfile(profile);
	}

	void updateCoreFields() {
		// Determine Action
		if (action == null)	action = 
				Boolean.TRUE.equals(truncate) ?	
				Action.INSERT : Action.UPDATE;
		if (action == Action.LOAD)    action = Action.INSERT;
		if (action == Action.REFRESH) action = Action.UPDATE;
		
		// Determine Source, Target and Name
		if (source == null)  source = (jobName != null) ? jobName : target;
		if (jobName == null) jobName = (target != null) ? target : source;
		if (target == null)	 target = (source != null) ? source : jobName;
		
	}

	void updateFromParent(JobConfig parent) {
		if (this.pageSize == null && parent.pageSize != null) {
			this.pageSize = parent.pageSize;
		}
	}
	
	void updateDateFields(DateTimeFactory dateFactory) {
		if (sinceExpr == null) {
			sinceDate = null;
		}
		else {
			sinceDate = dateFactory.getDate(sinceExpr);
		}
		
		if (createdExpr == null) {
			createdRange = null;
		}
		if (createdExpr != null) {
			setDateFactory(dateFactory);
			setCreated(createdExpr);
		}			
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
		DateTime defaultEnd = dateFactory.getStart();
		assert defaultEnd != null;
		if (node.isTextual()) {
			String s1 = node.asText();
			DateTime d1 = dateFactory.getDate(s1);
			this.createdRange = new DateTimeRange(d1, defaultEnd);				
		}
		else if (node.isArray()) {
			int len = node.size();
			String s1 = len > 0 ? node.get(0).asText() : null;
			String s2 = len > 1 ? node.get(1).asText() : null;
			DateTime d1 = s1 == null ? null : dateFactory.getDate(s1);
			DateTime d2 = s2 == null ? defaultEnd : dateFactory.getDate(s2);
			this.createdRange = new DateTimeRange(d1, d2);				
		}
		else
			throw new ConfigParseException("Invalid created: " + node);		
	}
		
	void updateFromProfile(ConnectionProfile profile) {		
		// AutoCreate defaults to True
		if (autoCreate == null) {
			if (anyLoadAction.contains(action))
				autoCreate = profile.getPropertyBoolean("datamart.autocreate", true);
			else
				autoCreate = false;
		}				
	}
		
	void validateFields() {
		if (action == Action.EXECUTE) {
			if (sql == null) configError("Missing SQL");
		}
		else {
			if (action == null) configError("Action not specified");
			if (source == null) configError("Source not specified");
			if (target == null) configError("Target not specified");
			if (jobName == null) configError("Name not specified");
		}
		booleanValidForActions("Truncate", truncate, EnumSet.of(Action.INSERT));
		booleanValidForActions("Drop", dropTable, EnumSet.of(Action.CREATE));
		validForActions("Created", createdRange, EnumSet.range(Action.INSERT, Action.SYNC));
		validForActions("Since", sinceDate, EnumSet.range(Action.INSERT, Action.UPDATE));
		validForActions("SQL", sql, EnumSet.of(Action.EXECUTE));
		
		if (sinceDate == null && sinceExpr != null)
			configError("Missing Since Date");
		if (createdRange == null & createdExpr != null)
			configError("Missing Created Range");
		
		if (getIncludeColumns() != null && getExcludeColumns() != null) 
			configError("Cannot specify both Columns and Exclude");
		
	}
		
	void booleanValidForActions(String name, Boolean value, EnumSet<Action> validActions) {
		if (Boolean.TRUE.equals(value)) {
			if (!validActions.contains(action))
				notValid(name, action);
		}
	}
	
	void validForActions(String name, Object value, EnumSet<Action> validActions)
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
