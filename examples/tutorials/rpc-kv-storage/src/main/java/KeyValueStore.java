import java.util.HashMap;
import java.util.Map;

// [START EXAMPLE]
public class KeyValueStore {

	private final Map<String, String> store = new HashMap<>();

	public String put(String key, String value) {
		return store.put(key, value);
	}

	public String get(String key) {
		return store.get(key);
	}
}
// [END EXAMPLE]
