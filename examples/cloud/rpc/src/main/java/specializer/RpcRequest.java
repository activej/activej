package specializer;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

public final class RpcRequest {
	private final int id;

	public RpcRequest(@Deserialize("id") int id) {
		this.id = id;
	}

	@Serialize
	public int getId() {
		return id;
	}
}
