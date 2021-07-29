import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

// [START EXAMPLE]
public class PutRequest {

	private final String key;
	private final String value;

	public PutRequest(@Deserialize("key") String key, @Deserialize("value") String value) {
		this.key = key;
		this.value = value;
	}

	@Serialize
	public String getKey() {
		return key;
	}

	@Serialize
	public String getValue() {
		return value;
	}
}
// [END EXAMPLE]
