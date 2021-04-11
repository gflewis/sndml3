package sndml.servicenow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.Action;
import sndml.datamart.Log4jProgressLogger;

public class FileWriter extends RecordWriter {

	public enum Format {Import, List};
	
	File file;
	PrintWriter writer;

	Format format = Format.Import; 

	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static void main(String[] args) throws Exception {
		Log.setGlobalContext();
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("prop").required(true).hasArg(true).
				desc("Profile file name (required)").build());
		options.addOption(Option.builder("t").longOpt("table").required(true).hasArg(true).
				desc("Table name").build());
		options.addOption(Option.builder("o").longOpt("output").required(false).hasArg(true).
				desc("Output file name").build());
		options.addOption(Option.builder("q").longOpt("query").required(false).hasArg(true).
				desc("Encoded query").build());
		DefaultParser parser = new DefaultParser();
		CommandLine cmdline = parser.parse(options,  args);
		String propfilename = cmdline.getOptionValue("p");
		String outfilename = cmdline.getOptionValue("o");
		String tablename = cmdline.getOptionValue("t");
		String querystring = cmdline.getOptionValue("q");
		Properties props = new Properties();
		File propfile = new File(propfilename);
		props.load(new FileInputStream(propfile));
		File outfile = (outfilename == null) ? null : new File(outfilename);
		Session session = new Session(props);
		Table table = session.table(tablename);
		TableReader reader = new RestTableReader(table);
		EncodedQuery query = querystring == null ? new EncodedQuery(table) :
			new EncodedQuery(table, querystring);
		reader.setFilter(query);
		FileWriter writer = new FileWriter(outfile);
		Metrics metrics = new Metrics(outfile.getName());
		ProgressLogger progress = new Log4jProgressLogger(reader, Action.INSERT);
//		reader.setWriter(writer, metrics);
		reader.prepare(writer, metrics, progress);
		writer.open(metrics);
		reader.call();
		writer.close(metrics);
	}

	public FileWriter(File file) {
		super();
		this.file = file;
	}

	public FileWriter setFormat(Format fmt) {
		this.format = fmt;
		return this;
	}
	
	@Override
	public FileWriter open(Metrics writerMetrics) throws IOException {
		writerMetrics.start();
		if (file == null)
			writer = new PrintWriter(System.out);
		else
			writer = new PrintWriter(file);
		if (format == Format.List) {
			writer.println("[");
			writer.flush();
		}
		return this;
	}

	@Override
	public synchronized void processRecords(
			RecordList recs, Metrics writerMetrics, ProgressLogger progressLogger) {
		assert writer != null;
		assert recs != null;
		for (TableRecord rec : recs) {
			processRecord(rec, writerMetrics);
		}
	}

	public void processRecord(TableRecord rec, Metrics writerMetrics) {	
		assert writer != null;
		assert rec != null;
		assert rec instanceof JsonRecord;
		if (format == Format.Import) {			
			writer.println(rec.asText(false));
		}
		else {
			writer.print(rec.asText(true));
			writer.println(",");
		}
		writer.flush();
		writerMetrics.incrementInserted();
	}

	@Override
	public void close(Metrics writerMetrics) {
		if (format == Format.List) writer.println("]");
		writer.close();
		writerMetrics.finish();
	}
	
}
