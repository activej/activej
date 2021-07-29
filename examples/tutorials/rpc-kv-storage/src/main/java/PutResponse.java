import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;

// [START EXAMPLE]
public class PutResponse {
	private final String previousValue;

	public PutResponse(@Deserialize("previousValue") String previousValue) {
		this.previousValue = previousValue;
	}

	@Serialize
	@SerializeNullable
	public String getPreviousValue() {
		return previousValue;
	}

	@Override
	public String toString() {
		return "{previousValue='" + previousValue + '\'' + '}';
	}
}
// [END EXAMPLE]
