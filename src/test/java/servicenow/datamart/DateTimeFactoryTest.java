package servicenow.datamart;

import static org.junit.Assert.*;
import java.util.Properties;
import org.junit.Test;

import servicenow.core.*;
import servicenow.datamart.DateTimeFactory;

public class DateTimeFactoryTest {

	DateTime START = new DateTime("2017-06-15 10:15:00");
	
	Properties getProperties() {
		Properties props = new Properties();
		props.setProperty("loader.start",  "2017-06-14 17:35:35");
		props.setProperty("loader.incident.start",  "2017-06-14 17:35:35");
		props.setProperty("loader.incident.finish",  "2017-06-14 18:20:20");
		return props;
	}
	
	DateTimeFactory getFactory() {
		return new DateTimeFactory(START, getProperties());
	}
	
	@Test
	public void testDateTimeFactory() throws Exception {
		DateTimeFactory factory = getFactory();
		assertEquals(START, factory.getDate("2017-06-15 10:15:00"));
		assertEquals(START, factory.getDate("start"));
		assertEquals(START, factory.getDate("START"));
		assertEquals(new DateTime("2017-06-15"), factory.getDate("today"));
		assertEquals(new DateTime("2017-06-14"), factory.getDate("today-1d"));
		assertEquals(new DateTime("2017-06-14"), factory.getDate("Today-1D"));
		assertEquals(new DateTime("2017-06-15 10:10:00"), factory.getDate("start-5m"));
		assertEquals(new DateTime("2017-06-15 10:10:00"), factory.getDate("-5m"));
		assertEquals(new DateTime("2017-06-14 10:15:00"), factory.getDate("-1d"));
		assertEquals(new DateTime("2017-06-14 11:15:00"), factory.getDate("-23h"));
		assertEquals(new DateTime("2017-06-14 17:35:35"), factory.getDate("last"));
		assertEquals(new DateTime("2017-06-14 17:35:35"), factory.getDate("loader.incident.start"));
	}

}
