package sndml.servicenow;

import java.io.PrintWriter;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Metrics {

	private final String name; // name as it appears in properties file; null if global
	private final Metrics parent;
	private Integer expected = null;
	private int input = 0;
	private int inserted = 0;
	private int updated = 0;
	private int deleted = 0;
	private int skipped = 0;
	private Date started = null;
	private Date finished = null;

	@SuppressWarnings("unused")
	private static Logger logger = LoggerFactory.getLogger(Metrics.class);
			
	public Metrics(String name) {
		this.name = name;
		this.parent = null;
	}
	
	public Metrics(String name, Metrics parent) {
		this.name = name;
		this.parent = parent;
	}
		
	public String getName() {
		return this.name;
	}
	
//	@Deprecated
//	public void setParent(Metrics parent) {
//		assert parent != null;
//		assert parent != this;
//		assert parent.parent == null || parent.parent.parent == null;
//		this.parent = parent;
//	}
	
	public boolean hasParent() {
		assert parent == null || parent != this; // object cannot be its own parent
		return parent != null;
	}
	
	public Metrics getParent() {
		assert parent == null || parent != this; // object cannot be its own parent
		return parent;
	}
	
	public synchronized Metrics start() {
		if (parent != null) parent.start();
		if (started == null) started = new Date();
		return this;
	}
	
	public synchronized Metrics finish() {
		finished = new Date();
		if (parent != null) parent.finish();
		return this;
	}
	
	public DateTime getStarted() {
		return started == null ? null : new DateTime(started);
	}
	
	public DateTime getFinished() {
		return finished == null ? null : new DateTime(finished);
	}
	
	public double getElapsedSec() {
		if (started == null || finished == null)
			return 0;
		else {
			double result = ((finished.getTime() - started.getTime()) / 1000.0);			
			return result;
		}
	}

	/**
	 * Set the number of rows that the reader is expected to return (expected input)
	 */
	public synchronized void setExpected(Integer value) {
		expected = value;
	}
	
	public synchronized boolean hasExpected() {
		return expected != null;
	}
			
	/**
	 * Return the number of rows that the reader is expected to return (expected input)
	 */
	public synchronized Integer getExpected() {
		return expected;
	}
		
	/**
	 * Return the number of rows read from the reader
	 */
	public int getInput() {
		return this.input;
	}
	
	public int getProcessed() {
		return getInserted() + getUpdated() + getDeleted() + getSkipped();
	}
	
	public int getInserted() {
		return this.inserted;
	}
	
	public int getUpdated() {
		return this.updated;
	}
	
	public int getDeleted() {
		return this.deleted;
	}
	
	public int getSkipped() {
		return this.skipped;
	}
	
	public void incrementInput() {
		addInput(1);
	}
	
	public void incrementInserted() {
		addInserted(1);
	}
	
	public void incrementUpdated() {
		addUpdated(1);
	}
	
	public void incrementDeleted() {
		addDeleted(1);
	}
	
	public void incrementSkipped() {
		addSkipped(1);
	}

	public synchronized void addInput(int count) {
		input += count;
		if (parent != null) parent.addInput(count);;
		
	}
	public synchronized void addInserted(int count) {
		inserted += count;
		if (parent != null) parent.addInserted(count);		
	}
	
	public synchronized void addUpdated(int count) {
		updated += count;
		if (parent != null) parent.addUpdated(count);
	}
	
	public synchronized void addDeleted(int count) {
		deleted += count;
		if (parent != null) parent.addDeleted(count);
	}
	
	public synchronized void addSkipped(int count) {
		skipped += count;
		if (parent != null) parent.addSkipped(count);
	}
	
	public synchronized void add(Metrics stats) {
		assert stats != null;
		assert stats.started != null;
		assert stats.finished != null;
		if (started == null || started.getTime() > stats.started.getTime()) started = stats.started;
		if (finished == null || finished.getTime() < stats.finished.getTime()) finished = stats.finished;
		inserted += stats.inserted;
		updated += stats.updated;
		deleted += stats.deleted;
		skipped += stats.skipped;
	}
		
	public void write(PrintWriter writer) {
		String prefix = (name == null ? "" : name + ".");
		writer.println(prefix + "start="     + getStarted());
		writer.println(prefix + "finish="    + getFinished());
		writer.println(prefix + "elapsed="   + String.format("%.1f", getElapsedSec()));
		writer.println(prefix + "inserted="  + String.valueOf(getInserted()));
		writer.println(prefix + "updated="   + String.valueOf(getUpdated()));
		writer.println(prefix + "deleted="   + String.valueOf(getDeleted()));
		writer.println(prefix + "skipped="   + String.valueOf(getSkipped()));
		writer.println(prefix + "processed=" + String.valueOf(getProcessed()));		
	}
	
	// Used for debugging
	public String toString() {
		return String.format(
			"%s[expected=%d input=%d inserted=%d updated=%d deleted=%d]",
			name == null ? "GLOBAL" : name,
			getExpected(), getInput(), getInserted(), getUpdated(), getDeleted());
	}
	
}
