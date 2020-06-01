import java.util.Map;

//[START EXAMPLE]
public interface RecordDAO {
	void add(Record record);

	void delete(int id);

	Record find(int id);

	Map<Integer, Record> findAll();
}
//[END EXAMPLE]
