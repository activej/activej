package adder;

import io.activej.common.Checks;
import io.activej.serializer.annotations.SerializeRecord;

import static io.activej.common.Checks.checkArgument;

public class AdderCommands {
	public static final boolean CHECK = Checks.isEnabled(AdderCommands.class);

	@SerializeRecord
	public record AddRequest(long userId, float delta) implements HasUserId {
		public AddRequest {
			if (CHECK) checkArgument(delta > 0);
		}
	}

	public enum AddResponse {
		INSTANCE
	}

	@SerializeRecord
	public record GetRequest(long userId) implements HasUserId {}

	@SerializeRecord
	public record GetResponse(float sum) {}

	public interface HasUserId {
		long userId();
	}
}
