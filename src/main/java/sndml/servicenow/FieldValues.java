package sndml.servicenow;

import java.text.DateFormat;
import java.util.Date;

/**
 * <p>This object contains a collection of name/value pairs.
 * It is used to insert or update ServiceNow tables.</p>
 * 
 * <p><b>Example:</b></p>
 * <pre>
 * FieldValues values = new FieldValues();
 * values.set("short_description", short_description);
 * values.set("assignment_group", assignment_group);
 * table.insert(values);
 * </pre>
 *
 */
public class FieldValues extends Parameters {

	private static final long serialVersionUID = 1L;
	
	public FieldValues() {
		super();
	}
	
	public FieldValues(String name, String value) {
		super();
		put(name, value);
	}
	
	public FieldValues set(String name, String value) {
		put(name, value);
		return this;
	}

	public FieldValues set(String name, boolean value) {
		return set(name, value ? "1" : "0");
	}
	
	public FieldValues set(String name, int value) {
		return set(name, Integer.toString(value));
	}
	
	public FieldValues set(String name, DateTime value) {
		return set(name, value.toString());
	}
	
	final static int SECONDS_PER_DAY = 24*60*60;

	public FieldValues setDuration(String name, Integer seconds) {
		if (seconds == null) return setNull(name);
		int days = seconds.intValue() / SECONDS_PER_DAY;
		int sec = seconds.intValue() - (days * SECONDS_PER_DAY);
		long millisec = sec * 1000;
		Date dt = new Date(millisec);
		DateFormat tf = DateTime.dateTimeFormat.get();
		String all = tf.format(dt);
		String dur = String.valueOf(days) + " " + all.substring(11);
		return set(name, dur);
	}	
	
	/**
	 * Copy the contents of another {@link FieldValues} object into this one.
	 * @param newvalues The values to be applied to this object.
	 * @return The modified object.
	 */
	public FieldValues set(FieldValues newvalues) {
		for (String name : newvalues.keySet()) {
			set(name, newvalues.get(name).toString());
		}
		return this;
	}
	
	/**
	 * Set a field value to null or empty.
	 */
	public FieldValues setNull(String name) {
		put(name, null);
		return this;
	}
	
}
