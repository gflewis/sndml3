package sndml.service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sndml.datamart.ConnectionProfile;
import sndml.datamart.JobConfig;
import sndml.servicenow.HttpMethod;
import sndml.servicenow.JsonRequest;
import sndml.servicenow.Session;

public class Scanner extends TimerTask {

	static Logger logger = LoggerFactory.getLogger(Scanner.class);
	
	final String scope;
	final String name;
	final Session session;
	final URI uri;

	ExecutorService workerPool;
	
	Scanner(ConnectionProfile profile) {
		this.session = profile.getSession();
		this.scope = profile.getProperty("loader.scope", "x_108443_sndml");
		this.name = profile.getProperty("loader.agent", "main");
		uri = session.getURI("api/" + scope + "/getrunlist/" + name);
	}
	
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(Option.builder("p").longOpt("profile").required(true).hasArg().
				desc("Profile file name (required)").build());
		CommandLine cmdline = new DefaultParser().parse(options, args);
		String profilename = cmdline.getOptionValue("p");
		ConnectionProfile profile = new ConnectionProfile(new File(profilename));
		Scanner scanner = new Scanner(profile);
		scanner.run();		
	}
	
	@Override
	public void run() {
		CloseableHttpClient client = session.getClient();
		JsonRequest request = new JsonRequest(client, uri, HttpMethod.GET, null);
		try {
			ObjectNode response = request.execute();
			logger.info(response.toPrettyString());
			ArrayNode runlist = (ArrayNode) response.get("result");
			for (int i = 0; i < runlist.size(); ++i) {
				ObjectNode obj = (ObjectNode) runlist.get(i);
				JobConfig config = new JobConfig(obj);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
