package sndml.servicenow;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

/**
 * Thread-safe class used to convert a JDOM Element to a string. 
 */
public class XmlFormatter {

	private static ThreadLocal<XMLOutputter> rawFormatter =
		new ThreadLocal<XMLOutputter>() {
			protected XMLOutputter initialValue() {
				return new XMLOutputter(Format.getRawFormat());
			}
		};
		
	private static ThreadLocal<XMLOutputter> prettyFormatter =
		new ThreadLocal<XMLOutputter>() {
			protected XMLOutputter initialValue() {
				return new XMLOutputter(Format.getPrettyFormat());
			}
		};
		
	/**
	 * Returns a JDOM Element pretty formatted as an XML string.  
	 */
	public static String format(Element element) {
		return format(element, true);		
	}

	/**
	 * Returns a JDOM Document pretty formatted as an XML string.  
	 */
	public static String format(Document document) {
		return format(document, true);
	}
	
	/**
	 * Returns a JDOM Element formatted as an XML string with the option of raw or pretty.
	 */
	public static String format(Element element, boolean pretty) {
		if (element == null) return null;
		XMLOutputter formatter = pretty ? 
			prettyFormatter.get() : rawFormatter.get();
		return formatter.outputString(element);		
	}
	
	/**
	 * Returns a JDOM Document formatted as an XML string with the option of raw or pretty.
	 */
	public static String format(Document document, boolean pretty) {
		if (document == null) return null;
		XMLOutputter formatter = pretty ? 
			prettyFormatter.get() : rawFormatter.get();
		return formatter.outputString(document);				
	}
	
}
