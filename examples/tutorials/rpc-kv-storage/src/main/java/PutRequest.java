import io.activej.serializer.annotations.SerializeRecord;

// [START EXAMPLE]
@SerializeRecord
public record PutRequest(String key, String value) {}
// [END EXAMPLE]
