import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeRecord;

// [START EXAMPLE]
@SerializeRecord
public record PutResponse(@SerializeNullable String previousValue) {}
// [END EXAMPLE]
