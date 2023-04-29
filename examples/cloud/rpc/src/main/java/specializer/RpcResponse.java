package specializer;

import io.activej.serializer.annotations.SerializeRecord;
import specializer.CookieBucketModule.CookieBucket;

@SerializeRecord
public record RpcResponse(int id, int cookieBucketHash) {
	public static RpcResponse create(RpcRequest request, CookieBucket cookieBucket) {
		return new RpcResponse(request.id(), cookieBucket.hashCode());
	}
}
