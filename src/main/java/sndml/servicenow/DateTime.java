package sndml.servicenow;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import sndml.datamart.Interval;

/**
 * An immutable thread-safe DateTime field in ServiceNow format.
 * This class can convert the value to or from a Java Date.
 * All DateTime fields are represented in GMT.
 */
@JsonSerialize(using = DateTimeSerializer.class)
public class DateTime implements Comparable<DateTime>, Comparator<DateTime> {

	public static final int DATE_ONLY = 10; // length of yyyy-MM-dd
	public static final int DATE_TIME = 19; // length of yyyy-MM-dd HH:mm:ss

	public static final int HOURS_PER_DAY = 24;
	public static final int MIN_PER_DAY = 60 * HOURS_PER_DAY;
	public static final int SEC_PER_DAY = 60 * MIN_PER_DAY;
	public static final int SEC_PER_HOUR = 60 * 60;
	public static final int SEC_PER_MINUTE = 60;
	public static final int SEC_PER_WEEK = 7 * SEC_PER_DAY;
	public static final int MILLISEC_PER_DAY = 1000 * SEC_PER_DAY;

	static ThreadLocal<DateFormat> dateOnlyFormat =
		new ThreadLocal<DateFormat>() {
			protected DateFormat initialValue() {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				df.setTimeZone(TimeZone.getTimeZone("GMT"));
				return df;
			}
		};

	static ThreadLocal<DateFormat> dateTimeFormat =
		new ThreadLocal<DateFormat>() {
			protected DateFormat initialValue() {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				df.setTimeZone(TimeZone.getTimeZone("GMT"));
				return df;
			}
		};

	private final String str;
	private final Date dt;

	/**
	 * Construct a {@link DateTime} from a string.
	 * InvalidDateTimeException will be thrown unless the value is
	 * "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss".
	 * @throws InvalidDateTimeException if length is not 10 or 19
	 * or if argument cannot be converted to a Date.
	 */
	public DateTime(String value) throws InvalidDateTimeException {
		this(value, value.length());
	}

	/**
	 * Construct a {@link DateTime} from a string.
	 * @param value String value to be converted
	 * @param fmtlen Specify 10 if value is "yyyy-MM-DD".
	 * Specify 19 if value is "yyyy-MM-dd HH:mm:ss".
	 * @throws InvalidDateTimeException if length is not 10 or 19
	 * or if argument cannot be converted to a Date.
	 */
	public DateTime(String value, int fmtlen) throws InvalidDateTimeException {
		DateFormat df;
		switch (fmtlen) {
		case DATE_ONLY : // 10
			df = dateOnlyFormat.get();
			break;
		case DATE_TIME : // 19
			df = dateTimeFormat.get();
			break;
		default :
			throw new InvalidDateTimeException(String.format("\"%s\" len=%d", value, fmtlen));
		}
		this.str = value;
		try {
			this.dt = df.parse(this.str);
		}
		catch (ParseException e) {
			throw new InvalidDateTimeException(value);
		}
	}

	/**
	 * Static function to convert a string to DateTime.
	 * @param s String to be converted.
	 * @return null if argument is null or zero length, otherwise a DateTime
	 */
	static public DateTime from(String s) {
		if (s == null) return null;
		if (s.length() == 0) return null;
		return new DateTime(s);
	}

	/**
	 * Construct a DateTime from a year, month and day.
	 * @param year - 4 digit year with century included
	 * @param month - month from 1 to 12
	 * @param day - day of the month
	 */
	private DateTime(int year, int month, int day) {
		this(String.format("%04d-%02d-%02d", year, month, day));
	}

	/**
	 * Construct a DateTime from a java.util.Date.
	 * Milliseconds will be truncated since DateTime only stores seconds.
	 */
	public DateTime(Date value) {
		// this only stores seconds so truncate any milliseconds
		this(value.getTime() / 1000);
	}

	/**
	 * <p>Construct a {@link DateTime} which is the number of seconds since 1970-01-01 00:00:00.</p>
	 * <p><b>Warning:</b> This constructor expects seconds, <b>not</b> milliseconds.</p>
	 */
	private DateTime(Long seconds) {
		boolean date_only = (seconds % SEC_PER_DAY == 0);
		DateFormat df = date_only ? dateOnlyFormat.get() : dateTimeFormat.get();
		this.dt = new Date(1000 * seconds);
		this.str = df.format(this.dt);
	}

	/**
	 * Make a copy of a DateTime
	 */
	public DateTime(DateTime orig) {
		this.dt = orig.dt;
		this.str = orig.str;
	}

	public String toFullString() {
		String str = toString();
		if (str.length() == DATE_ONLY) str += " 00:00:00";
		assert str.length() == DATE_TIME;
		return str;		
	}
	
	@Override
	public String toString() {
		return str;
	}

	public Date toDate() {
		return dt;
	}

	@Override
	public int compare(DateTime d1, DateTime d2) {
		return d1.compareTo(d2);
	}

	@Override
	public int compareTo(DateTime other) {
		return (int) (getSeconds() - other.getSeconds());
	}

	/**
	 * Return the number of milliseconds since 1970-01-01 00:00:00 GMT
	 * @return
	 */
	public long getMillisec() {
		return dt.getTime();
	}

	/**
	 * Return the number of seconds since 1970-01-01 00:00:00 GMT
	 */
	public long getSeconds() {
		return dt.getTime() / 1000;
	}

	public java.sql.Timestamp toTimestamp() {
		return new java.sql.Timestamp(getMillisec());
	}

	@Override
	public boolean equals(Object other) {
		if (other == null) return false;
		if (other instanceof DateTime) {
			DateTime d = (DateTime) other;
			return getMillisec() == d.getMillisec();
		}
		if (other instanceof Date) {
			Date d = (Date) other;
			return getMillisec() == d.getTime();
		}
		return this.toString().equals(other.toString());
	}

	public int hashCode() {
		return this.dt.hashCode();
	}

	public boolean before(DateTime other) {
		// no datetime is before the beginning of time
		if (other == null) return false;
		return getMillisec() < other.getMillisec();
	}

	public boolean after(DateTime other) {
		// no datetime is after the end of time
		if (other == null) return false;
		return getMillisec() > other.getMillisec();
	}

	public DateTime truncate() {
		return this.truncate(SEC_PER_DAY);
	}

	private DateTime truncate(int sec) {
		return new DateTime(sec * (this.getSeconds() / sec));
	}

	public DateTime truncate(Interval interval) {
		int y, m;
		switch (interval) {
		case MINUTE:
			return this.truncate(SEC_PER_MINUTE);
		case FIVEMIN:
			return this.truncate(SEC_PER_MINUTE * 5);
		case HOUR:
			return this.truncate(SEC_PER_HOUR);
		case DAY:
			return this.truncate(SEC_PER_DAY);
		case WEEK:
			// Jan 1, 1970 was a Thursday, so add 3 days
			return this.truncate(SEC_PER_WEEK).addSeconds(3 * SEC_PER_DAY);
		case MONTH:
			y = this.getYear();
			m = this.getMonth();
			return new DateTime(y, m, 1);
		case QUARTER:
			y = this.getYear();
			m = 3 * ((this.getMonth() - 1) / 3) + 1;
			return new DateTime(y, m, 1);
		case YEAR:
			y = this.getYear();
			return new DateTime(y, 1, 1);
		default:
			throw new AssertionError("Invalid interval");
		}
	}

	/**
	 * Return 4 digit year
	 */
	public int getYear() {
		return Integer.parseInt(this.toString().substring(0, 4));
	}

	/**
	 * Return month from 1 to 12 (January = 1)
	 */
	public int getMonth() {
		return Integer.parseInt(this.toString().substring(5, 7));
	}

	/**
	 * <p>Returns a new {@link DateTime} which is the original object incremented
	 * by the specified amount of time (or decremented if the argument
	 * is negative).</p>
	 * <p><b>Warning:</b> This method does <b>NOT</b> modify the original object.
	 * {@link DateTime} objects are immutable.</p>
	 */
	public DateTime addSeconds(int seconds) {
		return new DateTime(this.getSeconds() + seconds);
	}

	/**
	 * <p>Returns a new {@link DateTime} which is the original object decremented
	 * by the specified amount of time (or incremented if the argument
	 * is negative).</p>
	 * <p><b>Warning:</b> This method does <b>NOT</b> modify the original object.
	 * {@link DateTime} objects are immutable.</p>
	 */
	public DateTime subtractSeconds(int seconds) {
		return new DateTime(this.getSeconds() - seconds);
	}

	/**
	 * Return a new {@link DateTime} which is the original object incremented
	 * to the start of the next interval.
	 * Assumes original object is truncated.
	 */
	public DateTime incrementBy(Interval interval) {
		int y, m;
		switch (interval) {
		case YEAR:
			// return Jan 1 of next year
			return new DateTime(getYear() + 1, 1, 1);
		case QUARTER:
			// return 1st day of next quarter
			y = getYear();
			m = getMonth();
			m += 3;
			if (m > 12) {
				m = 1;
				y += 1;
			}
			return new DateTime(y, m, 1);
		case MONTH:
			// return 1st day of next month
			y = getYear();
			m = getMonth();
			m += 1;
			if (m > 12) {
				m = 1;
				y += 1;
			}
			return new DateTime(y, m, 1);
		case WEEK:
			return addSeconds(SEC_PER_WEEK);
		case DAY:
			return addSeconds(SEC_PER_DAY);
		case HOUR:
			return addSeconds(SEC_PER_HOUR);
		case FIVEMIN:
			return addSeconds(SEC_PER_MINUTE * 5);
		case MINUTE:
			return addSeconds(SEC_PER_MINUTE);
		default:
			throw new AssertionError("Invalid interval");
		}
	}

	/**
	 * Return a new {@link DateTime} which is the original object decremented
	 * to the start of the prior interval.
	 * Assumes original object is truncated.
	 */
	public DateTime decrementBy(Interval interval) {
		int y, m;
		switch (interval) {
		case YEAR:
			return new DateTime(getYear() - 1, 1, 1);
		case QUARTER:
			// return first day of prior quarter
			y = getYear();
			m = getMonth();
			m -= 3;
			if (m < 1) {
				m = 10;
				y -= 1;
			}
			return new DateTime(y, m, 1);
		case MONTH:
			// return first day of prior month
			y = getYear();
			m = getMonth();
			m -= 1;
			if (m < 1) {
				m = 12;
				y -= 1;
			}
			return new DateTime(y, m, 1);
		case WEEK:
			return subtractSeconds(SEC_PER_WEEK);
		case DAY:
			return subtractSeconds(SEC_PER_DAY);
		case HOUR:
			return subtractSeconds(SEC_PER_HOUR);
		case FIVEMIN:
			return subtractSeconds(SEC_PER_MINUTE * 5);
		case MINUTE:
			return subtractSeconds(SEC_PER_MINUTE);
		default:
			throw new AssertionError("Invalid interval");
		}
	}

	/**
	 * Returns the current time.
	 * @return Current datetime.
	 */
	public static DateTime now() {
		return new DateTime(new Date());
	}

	/**
	 * <p>Returns the current local time adjusted by a slight lag.</p>
	 * <p>This function is used to compensate for bugs that can occur if
	 * the clock on the local machine is running faster than the clock
	 * on the ServiceNow instance.</p>
	 */
	@Deprecated
	public static DateTime now(int lagSeconds) {
		return now().subtractSeconds(lagSeconds);
	}

	/**
	 * Returns the current GMT date.
	 * @return Midnight of the current GMT date.
	 */
	public static DateTime today() {
		return now().truncate();
//		DateTime now = new DateTime(new Date());
//		String trunc = now.toString().substring(0, 11)  + "00:00:00";
//		DateTime result = new DateTime(trunc);
//		return result;
	}

}
