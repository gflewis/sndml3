package sndml.datamart;

import java.io.*;
import java.util.*;
import org.jdom2.*;

/**
 * Contains a mapping between the Glide field name and the SQL field name
 * as loaded from the <b>fieldnames</b> element of the <b>sqltemplates.xml</b> file.
 * @author Giles Lewis
 *
 */
public class NameMap {

	Map<String,String> toGlide = new HashMap<String,String>();
	Map<String,String> toSql = new HashMap<String,String>();

	public NameMap(Element fieldnames) {
		loadFromXML(fieldnames);
	}
	
	public String getGlideName(String sqlName) {
		return toGlide.get(sqlName);
	}
	
	public String getSqlName(String glideName) {
		return toSql.get(glideName);
	}
	
	public void printMap(PrintStream out) {
		Set<String> keys = toSql.keySet();
		Iterator<String> iter = keys.iterator();
		while (iter.hasNext()) {
			String key = iter.next();
			String val = toSql.get(key);
			out.println(key + "=" + val);
		}
	}

	public void loadFromXML(Element fieldnames) {
		List<Element> children = fieldnames.getChildren("namemap");
		ListIterator<Element> iter = children.listIterator();
		while (iter.hasNext()) {
			Element namemap = iter.next();
			String glidename = namemap.getAttribute("glidename").getValue();
			String sqlname = namemap.getTextTrim();
			this.toSql.put(glidename, sqlname);
			this.toGlide.put(sqlname, glidename);			
		}
	}
	
}
