package sndml.datamart;

import java.util.EnumSet;

@Deprecated
public class JobConfigValidator {

	final JobConfig job;
	final Action action;
	
	JobConfigValidator(JobConfig job, DateTimeFactory factory) {
		this.job = job;
		this.action = job.action;
	}
	
	void validate() throws ConfigParseException {
		if (action == null) configError("Action not specified");
		if (job.getSource() == null) configError("Source not specified");
		if (job.getTarget() == null) configError("Target not specified");
		if (job.getName() == null) configError("Name not specified");
		validate("Truncate", job.truncate, EnumSet.of(Action.INSERT));
		validate("Drop", job.dropTable, EnumSet.of(Action.CREATE));
		validate("Created", job.createdRange, EnumSet.range(Action.INSERT, Action.SYNC));
		validate("Since", job.sinceDate, EnumSet.range(Action.INSERT, Action.UPDATE));
		if (job.getIncludeColumns() != null && job.getExcludeColumns() != null) 
			configError("Cannot specify both Columns and Exclude");		
	}
	
	
	void validate(String name, Object value, EnumSet<Action> values)
		throws ConfigParseException
	{
		if (value != null) {
			if (!values.contains(action))
				notValid(name);
		}		
	}
	
	void notValid(String option) throws ConfigParseException {
		String msg = option + " not valid with Action: " + action.toString();
		configError(msg);
	}

	void configError(String msg) {
		throw new ConfigParseException(msg);
	}

	
}
