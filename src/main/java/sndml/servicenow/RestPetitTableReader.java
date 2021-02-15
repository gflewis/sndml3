package sndml.servicenow;

/**
 * <p>A {@link RestTableReader} which attempts to read all qualifying records in a single
 * Web Services call.
 * </p>
 * <p>A normal {@link RestTableReader} will continue fetching pages until an empty page
 * is returned, or the number of records reaches the expected number based on the stats.
 * A {@link RestPetitTableReader} skips the stats and it stops reading as soon as the
 * number of records returned is smaller than the page size. However, this class could 
 * miss some records if ACLs are in place. This class should only be used if the page size 
 * is much larger than the maximum number of records that are expected.
 * </p>
 *
 */
public class RestPetitTableReader extends RestTableReader {

	public RestPetitTableReader(Table table) {
		super(table);
		this.statsEnabled = false;
		this.orderBy = OrderBy.NONE;
	}

	@Override
	protected boolean isFinished(int pageRows, int totalRows) {
		if (pageRows == 0 || pageRows < this.pageSize) return true;
		if (statsEnabled && totalRows >= getExpected()) return true;
		return false;
	}
}
