package servicenow.api;

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
