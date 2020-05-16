package servicenow.datamart;

import servicenow.api.*;

public class JobConfig extends Config {

	private Map items;
	private LoaderConfig parent;
	private String name;
	private String source;
	private String target;
	private LoaderAction action;
	private Boolean truncate;
	private DateTimeRange created;
	private DateTime since;
	private EncodedQuery filter;
	// orderBy removed 2020-01-24
	// private String orderBy;
	private DateTime.Interval partition;
	private Integer pageSize;
	private Integer minRows;
	private Integer maxRows;
	private String sqlBefore;
	private String sqlAfter;
	private FieldNames columns;
	private Integer threads;
	private final DateTimeFactory dateFactory;

	JobConfig(Table table) {
		this.name = table.getName();
		this.dateFactory = new DateTimeFactory();
	}
	
	JobConfig(LoaderConfig parent, Object config) throws ConfigParseException {
		this.parent = parent;
		this.dateFactory = new DateTimeFactory();
		if (isMap(config)) {
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
			    		case "sync":   this.action = LoaderAction.SYNC; break;
			    		case "create": this.action = LoaderAction.CREATE; break;
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
			    case "columns":
			    		this.columns = new FieldNames(val.toString());
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
	
	void validate() throws ConfigParseException {
		if (name == null && source == null && target == null) 
			configError("Must specify at least one of Name, Source, Target");
		switch (getAction()) {
		case UPDATE:
			break;
		case INSERT:
			break;
		case PRUNE:
			if (getTruncate()) notValid("Truncate");
			if (created != null) notValid("Created");
			if (filter != null) notValid("Filter");
			if (threads != null) notValid("Threads");
			if (partition != null) notValid("Partition");
			break;
		case SYNC:
			if (getTruncate()) notValid("Truncate");
			if (since != null) notValid("Since");
			if (filter != null) notValid("Filter");
			break;
		case CREATE:
			if (getTruncate()) notValid("Truncate");
			if (created != null) notValid("Created");
			if (filter != null) notValid("Filter");
			if (threads != null) notValid("Threads");
			if (partition != null) notValid("Partition");			
			break;		
		}
	}
	
	void notValid(String option) {
		String msg = option + " not valid with Action: " + getAction().toString();
		configError(msg);
	}

	void configError(String msg) {
		throw new ConfigParseException(msg);
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
	
	void setCreated(DateTimeRange value) {
		this.created = value;
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
	
	void setFilter(EncodedQuery value) {
		this.filter = value;
	}
	
	EncodedQuery getFilter() { return this.filter; }
	FieldNames getColumns()	 { return this.columns; }
	String  getSqlBefore()   { return this.sqlBefore; }
	String  getSqlAfter()    { return this.sqlAfter; }

	Integer getPageSize() {
		if (pageSize != null) return pageSize;
		if (parent != null) return parent.getPageSize();
		return null;
	}
	
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
