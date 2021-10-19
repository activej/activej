package specializer;

import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import specializer.CookieBucketModule.CookieBucket;

public class RpcResponse implements Promisable<RpcResponse> {
	private final int id;
	private final int cookieBucketHash;

	public RpcResponse(@Deserialize("id") int id, @Deserialize("cookieBucketHash") int cookieBucketHash) {
		this.id = id;
		this.cookieBucketHash = cookieBucketHash;
	}

	public static RpcResponse create(RpcRequest request, CookieBucket cookieBucket) {
		return new RpcResponse(request.getId(), cookieBucket.hashCode());
	}

	@Serialize
	public int getId() {
		return id;
	}

	@Serialize
	public int getCookieBucketHash() {
		return cookieBucketHash;
	}

	@Override
	public Promise<RpcResponse> promise() {
		return Promise.of(this);
	}
}
