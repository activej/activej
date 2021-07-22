package banner;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public class BannerCommands {
	public static final class PutRequest {
		private final long userId;
		private final Set<Integer> bannerIds;

		public PutRequest(@Deserialize("userId") long userId, @Deserialize("bannerIds") Set<Integer> bannerIds) {
			this.userId = userId;
			this.bannerIds = bannerIds;
		}

		@Serialize(order = 1)
		public long getUserId() {
			return userId;
		}

		@Serialize(order = 2)
		public Set<Integer> getBannerIds() {
			return bannerIds;
		}
	}

	public enum PutResponse {
		INSTANCE
	}

	public static final class GetRequest {
		private final long userId;

		public GetRequest(@Deserialize("userId") long userId) {
			this.userId = userId;
		}

		@Serialize(order = 1)
		public long getUserId() {
			return userId;
		}
	}

	public static final class GetResponse {
		@Nullable
		private final Set<Integer> bannerIds;

		public GetResponse(@Deserialize("bannerIds") @Nullable Set<Integer> bannerIds) {
			this.bannerIds = bannerIds;
		}

		@Serialize(order = 1)
		@SerializeNullable
		@Nullable
		public Set<Integer> getBannerIds() {
			return bannerIds;
		}
	}

	public static final class IsBannerSeenRequest {
		private final long userId;
		private final int bannerId;

		public IsBannerSeenRequest(@Deserialize("userId") long userId, @Deserialize("bannerId") int bannerId) {
			this.userId = userId;
			this.bannerId = bannerId;
		}

		@Serialize(order = 1)
		public long getUserId() {
			return userId;
		}

		@Serialize(order = 2)
		public int getBannerId() {
			return bannerId;
		}
	}
}
