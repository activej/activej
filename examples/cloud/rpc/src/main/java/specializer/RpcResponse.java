package specializer;

import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.serializer.annotations.SerializeRecord;
import org.jetbrains.annotations.NotNull;
import specializer.CookieBucketModule.CookieBucket;

@SerializeRecord
public record RpcResponse(int id, int cookieBucketHash) implements Promisable<RpcResponse> {

	public static @NotNull RpcResponse create(RpcRequest request, CookieBucket cookieBucket) {
		return new RpcResponse(request.id(), cookieBucket.hashCode());
	}

	@Override
	public Promise<RpcResponse> promise() {
		return Promise.of(this);
	}
}
