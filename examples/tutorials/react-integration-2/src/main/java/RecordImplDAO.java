import java.util.HashMap;
import java.util.Map;

//[START EXAMPLE]
public final class RecordImplDAO implements RecordDAO {
	private Map<Integer, Record> recordMap = new HashMap<>();
	private int counter;

	public void add(Record record) {
		recordMap.put(counter++, record);
	}

	@Override
	public void delete(int id) {
		recordMap.remove(id);
	}

	@Override
	public Record find(int id) {
		return recordMap.get(id);
	}

	@Override
	public Map<Integer, Record> findAll() {
		return recordMap;
	}
}
//[END EXAMPLE]
