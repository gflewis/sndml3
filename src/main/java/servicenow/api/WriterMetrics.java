package servicenow.api;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

public class WriterMetrics {

	private int inserted = 0;
	private int updated = 0;
	private int deleted = 0;
	private int skipped = 0;
	private Date started = null;
	private Date finished = null;
	private WriterMetrics parent = null;

	public void setParent(WriterMetrics parent) {
		this.parent = parent;
	}
	
	public WriterMetrics getParent() {
		return this.parent;
	}
	
	public synchronized WriterMetrics start() {
		if (parent != null) parent.start();
		if (started == null) started = new Date();
		return this;
	}
	
	public synchronized WriterMetrics finish() {
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
	
	public int getElapsedSec() {
		if (started == null || finished == null)
			return 0;
		else
			return (int) ((finished.getTime() - started.getTime()) / 1000);
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
	
	@Deprecated
	public synchronized void add(WriterMetrics stats) {
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
	
	public void write(PrintWriter writer) throws IOException {
		write(writer, null);
	}

	public void write(PrintWriter writer, String prefix) throws IOException {
		assert writer != null;
		if (prefix == null) prefix = "";
		if (prefix.length() > 0) prefix = prefix + ".";
		writer.println(prefix + "start="     + getStarted());
		writer.println(prefix + "finish="    + getFinished());
		writer.println(prefix + "elapsed="   + String.valueOf(getElapsedSec()));
		writer.println(prefix + "inserted="  + String.valueOf(getInserted()));
		writer.println(prefix + "updated="   + String.valueOf(getUpdated()));
		writer.println(prefix + "deleted="   + String.valueOf(getDeleted()));
		writer.println(prefix + "skipped="   + String.valueOf(getSkipped()));
		writer.println(prefix + "processed=" + String.valueOf(getProcessed()));		
	}
	
}
