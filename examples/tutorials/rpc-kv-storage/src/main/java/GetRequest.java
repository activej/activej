import io.activej.serializer.annotations.SerializeRecord;

// [START EXAMPLE]
@SerializeRecord
public record GetRequest(String key) {}
// [END EXAMPLE]
