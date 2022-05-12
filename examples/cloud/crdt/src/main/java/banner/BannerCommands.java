package banner;

import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeRecord;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public class BannerCommands {

	@SerializeRecord
	public record PutRequest(long userId, Set<Integer> bannerIds) {}

	public enum PutResponse {
		INSTANCE
	}

	@SerializeRecord
	public record GetRequest(long userId) {}

	@SerializeRecord
	public record GetResponse(@SerializeNullable @Nullable Set<Integer> bannerIds) {}

	@SerializeRecord
	public record IsBannerSeenRequest(long userId, int bannerId) {}
}
