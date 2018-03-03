package servicenow.datamart;

import servicenow.api.*;

import java.util.regex.Pattern;

public class TableConfig extends Config {

	private Map items;
	private String name;
	private String source;
	private String target;
	private LoaderAction action;
	private Boolean truncate;
	private DateTimeRange created;
//	private DateTimeRange updated;
	private DateTime since;
	private EncodedQuery filter;
	private String orderBy;
	private DateTime.Interval partition;
	private Integer pageSize;
	private Integer minRows;
	private Integer maxRows;
	private String sqlBefore;
	private String sqlAfter;
	private Integer threads;
	private DateTimeFactory dateFactory;

	TableConfig(Table table) {
		this.name = table.getName();
	}
	
	TableConfig(LoaderConfig parent, Object config) throws ConfigParseException {
		dateFactory = new DateTimeFactory();
		if (isMap(config)) {
			assert parent != null;
			items = new Config.Map(config);
			for (String origkey : items.keySet()) {
			    Object val = items.get(origkey);
			    String key = origkey.toLowerCase(); 
			    switch (key) {
			    case "name":
			    		this.name = val.toString(); 
			    		break;
			    case "source":
			    		this.source = val.toString(); 
			    		break;
			    case "target": 
			    		this.target = val.toString(); 
			    		break;
			    case "action":
			    		switch (val.toString().toLowerCase()) {
			    		case "update": this.action = LoaderAction.UPDATE; break;
			    		case "insert": this.action = LoaderAction.INSERT; break;
			    		case "prune":  this.action = LoaderAction.PRUNE; break;
			    		default:
						throw new ConfigParseException("Not recognized: " + val.toString());			    			
			    		}
			    		break;
			    case "truncate":
			    		this.truncate = (Boolean) val; 
			    		break;
			    case "created":
			    		this.created = asDateRange(val); 
			    		break;
			    case "since":
			    		this.since = asDate(val); 
			    		break;
			    case "filter":
			    		this.filter = new EncodedQuery(val.toString()); 
			    		break;
			    case "partition":
			    		this.partition = asInterval(val); 
			    		break;
			    case "orderby":
			    		this.orderBy = val.toString(); 
			    		break;
			    case "pagesize" :
			    		this.pageSize = asInteger(val); 
			    		break;
			    case "sqlbefore" :
			    		this.sqlBefore = val.toString(); 
			    		break;
			    case "sqlafter" :
			    		this.sqlAfter = val.toString(); 
			    		break;
			    case "minrows" :
			    		this.minRows = asInteger(val);
			    		break;
			    case "maxrows" :
			    		this.maxRows = asInteger(val);
			    		break;
			    case "threads" :
			    		this.threads = asInteger(val); 
			    		break;
			    	default:
			    		throw new ConfigParseException("Not recognized: " + origkey);
			    }
			}
		}
		else {
			if (config instanceof String)
				name = (String) config;
			else
				throw new ConfigParseException("Not recognized: " + config.toString());
		}		
	}
	
	void configError(String msg) {
		throw new ConfigParseException(msg);
	}
	
	void validate() throws ConfigParseException {
		if (name == null && source == null && target == null) 
			configError("Must specify at least one of Name, Source, Target");
		if (getAction().equals(LoaderAction.PRUNE)) {
			if (created != null) configError("Created not valid with Action: Prune");
			if (filter != null)  configError("Filter not valid with Action: Prune");
			if (orderBy != null) configError("OrderBy not valid with Action: Prune");
			if (threads != null) configError("Threads not valid with Action: Prune");			
		}
		if (orderBy != null && !Pattern.matches("(\\+|\\-)?\\w+", orderBy))
			configError("Invalid OrderBy");				
	}
			
	String getName() throws ConfigParseException {
		if (this.name != null) return this.name;
		if (this.target != null) return this.target;
		if (this.source != null) return this.source;
		throw new ConfigParseException("Name not specified");
	}

	String getSource() throws ConfigParseException {
		if (this.source != null) return this.source;
		if (this.target != null) return this.target;
		if (this.name != null) return this.name;
		throw new ConfigParseException("Source not specified");
	}
	
	String getTargetName() throws ConfigParseException {
		if (this.target != null) return this.target;
		if (this.source != null) return this.source;
		if (this.name != null) return this.name;
		throw new ConfigParseException("Target not specified");
	}

	void setAction(LoaderAction action) {
		this.action = action;
	}
	
	LoaderAction getAction() {
		if (this.action == null) 
			return this.getTruncate() ? LoaderAction.INSERT : LoaderAction.UPDATE;
		else
			return this.action;
	}
	
	void setTruncate(boolean truncate) {
		this.truncate = truncate;
	}
	
	boolean getTruncate() {
		return this.truncate == null ? false : this.truncate.booleanValue();
	}
	
	DateTimeRange getCreated() {
		if (this.created == null)
			return getDefaultRange();
		else
			return this.created;
	}
		
	void setSince(DateTime since) {
		this.since = since;
	}
	
	DateTime getSince()      { 
		return this.since;	
	}
	
	EncodedQuery getFilter() { return this.filter; }
	String  getOrderBy()     { return this.orderBy; }	
	String  getSqlBefore()   { return this.sqlBefore; }
	String  getSqlAfter()    { return this.sqlAfter; }
	Integer getPageSize()    { return this.pageSize;	}
	Integer getMinRows()     { return this.minRows; }
	Integer getMaxRows()     { return this.maxRows; }
	Integer getThreads()     { return this.threads; }
	
	DateTime.Interval getPartitionInterval() {
		return this.partition;
	}
	
	DateTime.Interval asInterval(Object obj) throws ConfigParseException {
		DateTime.Interval result;
		try {
			result = DateTime.Interval.valueOf(obj.toString().toUpperCase());
		}
		catch (IllegalArgumentException e) {
			throw new ConfigParseException("Invalid partition: " + obj.toString());
		}
		return result;
	}
	
	DateTime asDate(Object obj) {
		return dateFactory.getDate(obj);
	}
	
	DateTimeRange asDateRange(Object obj) throws ConfigParseException {
		DateTime start, end;
		end = dateFactory.getStart();
		if (isList(obj)) {
			List dates = new Config.List(obj);
			if (dates.size() < 1 || dates.size() > 2) 
				throw new ConfigParseException("Invalid date range: " + obj.toString());
			start = dateFactory.getDate(dates.get(0));
			if (dates.size() > 1) end = dateFactory.getDate(dates.get(1));
		}
		else {
			start = dateFactory.getDate(obj);
		}
		return new DateTimeRange(start, end);
	}
	
	DateTimeRange getDefaultRange() {
		assert dateFactory != null;
		return new DateTimeRange(null, dateFactory.getStart());
	}
	
}
