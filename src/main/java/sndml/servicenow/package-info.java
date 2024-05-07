/**
 * Classes used to access a ServiceNow instance.
 * Most significantly, this package contains classes which know how to 
 * efficiently and reliably read and process
 * large amounts of data from ServiceNow.
 * <p/>
 * This package implements the Java "visitor" pattern using a pair of abstract classes:
 * {@link TableReader} and {@link RecordWriter}.
 * Classes extended from {@link TableReader} are implented in this package, 
 * whereas classes extended from {@link RecordWriter} are implemented in the {@link sndml.loader} package.
 * The most common {@link TableReader} is a {@link RestTableReader}.
 * This package also contains some older classes (<i>e.g.</i> {@link SoapKeySetTableReader})
 * which implement the SOAP API, but are no longer used. 
 * 
 */
package sndml.servicenow;