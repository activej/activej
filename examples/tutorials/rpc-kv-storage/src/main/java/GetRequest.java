import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

// [START EXAMPLE]
public class GetRequest {

	private final String key;

	public GetRequest(@Deserialize("key") String key) {
		this.key = key;
	}

	@Serialize
	public String getKey() {
		return key;
	}
}
// [END EXAMPLE]
