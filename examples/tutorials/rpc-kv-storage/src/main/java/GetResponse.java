import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;

// [START EXAMPLE]
public class GetResponse {
	private final String value;

	public GetResponse(@Deserialize("value") String value) {
		this.value = value;
	}

	@Serialize
	@SerializeNullable
	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "{value='" + value + '\'' + '}';
	}
}
// [END EXAMPLE]
