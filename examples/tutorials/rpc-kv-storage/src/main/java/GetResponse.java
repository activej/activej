import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeRecord;

// [START EXAMPLE]
@SerializeRecord
public record GetResponse(@SerializeNullable String value) {}
// [END EXAMPLE]
