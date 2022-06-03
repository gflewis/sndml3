package sndml.datamart;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.regex.Pattern;

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
	
	public DateTime start;
	public DateTime last;
	public RecordKey sys_id;
	public String number;
	@JsonProperty("name") public String jobName;
	public String source;
	public String target;
	public Action action;
	public RecordKey doc_id; // Action SINGLE only
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
	@Deprecated public String sqlBefore;
	@Deprecated public String sqlAfter;
	public Boolean autoCreate;
	@JsonIgnore public FieldNames includeColumns;
	public Integer threads;
	
	static EnumSet<Action> anyLoadAction =
			EnumSet.of(Action.INSERT, Action.UPDATE, Action.SYNC);
	
	public String getName() { return jobName; }
	public String getSource() {	return source;	}
	public String getTarget() {	return this.target;	}
	
	public RecordKey getSysId() { return this.sys_id; }
	public String getNumber() { return this.number; }
	public Action getAction() { return action; }
	public RecordKey getDocID() { return doc_id; }
	boolean getTruncate() {	return this.truncate == null ? false : this.truncate.booleanValue(); }
	boolean getDropTable() { return this.dropTable == null ? false : this.dropTable.booleanValue(); }
	DateTime getSince() { return this.sinceDate; }
	
	DateTimeRange getCreatedRange(DatePart datePart) { 
		DateTimeRange range = 
			datePart == null ? createdRange : datePart.intersect(createdRange);
		return range;
	}
	
	DateTimeRange getUpdatedRange() {
		if (sinceExpr == null) return null;
		return new DateTimeRange(getSince(), null);	
	}
	
	EncodedQuery getFilter(Table table) {
		if (filter == null) return null;
		return new EncodedQuery(table, filter);		
	}
		
	Interval getPartitionInterval() { return this.partition; }
	
	FieldNames getIncludeColumns() { return this.includeColumns; }
	
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
	
	void setColumns(String columnNames) {
		includeColumns = new FieldNames();
		includeColumns.add("sys_id");
		includeColumns.add("sys_created_on");
		includeColumns.add("sys_updated_on");
		FieldNames temp = new FieldNames(columnNames);
		for (String name : temp) {
			if (!includeColumns.contains(name)) includeColumns.add(name);
		}
	}
	
	FieldNames getColumns() {
		return this.includeColumns;
	}
			
	DateTimeRange getDefaultRange() {
		assert this.start != null;
		return new DateTimeRange(null, this.start);
	}

	void initializeAndValidate(ConnectionProfile profile, DateCalculator dateCalculator) {
		initialize(profile, dateCalculator);
		validate();		
	}
	
	void initialize(ConnectionProfile profile, DateCalculator dateCalculator) {
		updateCoreFields();
		updateDateFields(dateCalculator);
		if (profile != null) updateFromProfile(profile);
	}

	/**
	 * Set various default values
	 */
	private void updateCoreFields() {
		// Determine Action
		if (action == null)	action = 
				Boolean.TRUE.equals(truncate) ?	
				Action.INSERT : Action.UPDATE;
		if (action == Action.LOAD)    action = Action.INSERT;
		if (action == Action.REFRESH) action = Action.UPDATE;
		
		if (jobName == null && number != null) jobName = number;

		if (action == Action.EXECUTE) {
			if (jobName == null) jobName = "execute";
		}
		else {			
			// Determine Source, Target and Name
			if (source == null)  source = (jobName != null) ? jobName : target;
			if (jobName == null) jobName = (target != null) ? target : source;
			if (target == null)	 target = (source != null) ? source : jobName;		
		}
	}
	
	private void updateDateFields(DateCalculator calculator) {
		if (calculator == null) calculator = new DateCalculator();
		
		if (Objects.isNull(start))
			start = calculator.getStart();
		else
			calculator.setStart(start);		
		
		if (last != null) calculator.setLast(last);
		
		if (sinceExpr == null) {
			sinceDate = null;
		}
		else {
			sinceDate = calculator.getDate(sinceExpr);
		}
		
		if (createdExpr == null) {
			createdRange = null;
		}
		if (createdExpr != null) {
			setCreated(createdExpr, calculator);
		}			
	}

	@JsonIgnore
	void setCreated(DateTimeRange value) {
		this.createdRange = value;
	}
		
	private void setCreated(JsonNode node, DateCalculator calculator) {
		assert node != null;
		assert calculator != null;
		DateTime defaultEnd = calculator.getStart();
		assert defaultEnd != null;
		if (node.isTextual()) {
			String s1 = node.asText();
			DateTime d1 = calculator.getDate(s1);
			this.createdRange = new DateTimeRange(d1, defaultEnd);				
		}
		else if (node.isArray()) {
			int len = node.size();
			String s1 = len > 0 ? node.get(0).asText() : null;
			String s2 = len > 1 ? node.get(1).asText() : null;
			DateTime d1 = s1 == null ? null : calculator.getDate(s1);
			DateTime d2 = s2 == null ? defaultEnd : calculator.getDate(s2);
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
		
	void validate() {
		if (action == null) configError("Action not specified");
		if (jobName == null) configError("Name not specified");
		
		if (action == Action.EXECUTE) {
			if (sql == null) configError("Missing SQL");
		}
		else {
			if (source == null) configError("Source not specified");
			if (target == null) configError("Target not specified");
			if (!Pattern.compile("[a-z0-9_]+").matcher(source).matches())
				configError("Invalid source: " + source);
			if (!Pattern.compile("[A-Za-z0-9_]+").matcher(target).matches())
				configError("Invalid target: " + target);
		}
				
		booleanValidForActions("Truncate", truncate, EnumSet.of(Action.INSERT));
		booleanValidForActions("Drop", dropTable, EnumSet.of(Action.CREATE));
		validForActions("Created", createdRange, Action.INSERT_UPDATE_SYNC);
		validForActions("Partition", partition, Action.INSERT_UPDATE_SYNC);
		validForActions("Filter", filter, Action.INSERT_UPDATE_SYNC);
		validForActions("Since", sinceDate, Action.INSERT_UPDATE_PRUNE);
		validForActions("SQL", sql, Action.EXECUTE_ONLY);
		validForActions("Doc_ID", doc_id, Action.SINGLE_ONLY);
		
		if (sinceExpr != null && sinceDate == null)
			configError("Missing Since Date");
		if (createdExpr != null && createdRange == null)
			configError("Missing Created Range");
		if (action == Action.SINGLE && doc_id == null)
			configError("Missing doc_id");
		
		if (threads != null && partition == null)
			configError("Threads only valid with Partition");
		
		if (sqlBefore != null) logger.warn(Log.INIT, "Deprecated option: SQLBefore");
		if (sqlAfter != null) logger.warn(Log.INIT, "Deprecated option: SQLAfter");		
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
	
	public TableReader createReader(Table table, Database db) throws IOException {
		return createReader(table, db, null, false);
	}
	
	public TableReader createReader(Table table, Database db, DatePart datePart, boolean createNewSession) 
			throws IOException {

		assert table != null;
		String sqlTableName = getTarget();
		String jobName = getName();
		validate();
		assert jobName != null;
		String partName = Objects.isNull(datePart) ? null : datePart.getName();
		String readerName = Objects.isNull(partName) ? jobName : jobName + "." + partName;
		
		Table myTable = table;
		if (createNewSession) {
			Session newSession = table.getSession().duplicate();
			myTable = newSession.table(table.getName());
		}
		TableReader reader;
		if (action == Action.SYNC) {
			// Database connection is required for Synchronizer only
			assert db != null;
			reader = new Synchronizer(myTable, db, sqlTableName, readerName);
		}
		else {
			reader = new RestTableReader(myTable);
		}
		reader.setReaderName(readerName);
		reader.setPartName(partName);
		reader.setFilter(getFilter(myTable));		
		reader.setCreatedRange(getCreatedRange(datePart));		
		reader.setUpdatedRange(getUpdatedRange());
		reader.setFilter(getFilter(myTable));
		reader.setFields(getColumns());
		reader.setPageSize(getPageSize());
		reader.setMaxRows(getMaxRows());
		return reader;
	}
	
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		if (sys_id != null) node.put("sys_id",  sys_id.toString());
		if (number != null) node.put("number",  getNumber());
		node.put("start",  this.start.toString());
		if (this.last != null) node.put("last", this.last.toString());
		node.put("name", this.jobName);
		node.put("source", this.source);
		node.put("target", this.target);
		node.put("action", this.action.toString());
		if (doc_id != null) node.put("doc_id", doc_id.toString());
		if (getTruncate()) node.put("truncate", true);
		if (getDropTable()) node.put("drop", true);
		if (getAutoCreate()) node.put("autocreate", getAutoCreate());
		if (sinceExpr != null) 
			node.put("since", getSince().toString());
		if (createdRange != null) 
			node.set("created", getCreatedRange(null).toJsonNode());
		if (getPartitionInterval() != null) 
			node.put("partition",  getPartitionInterval().toString());
		if (filter != null) node.put("filter",this.filter);
		if (includeColumns != null) node.put("columns", includeColumns.toString());
		if (minRows != null) node.put("minrows", minRows);
		if (maxRows != null) node.put("maxrows", maxRows);
		String yaml;
		try {
			yaml = mapper.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new ConfigParseException(e);
		}
		return yaml;
	}
		
}
