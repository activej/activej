package specializer;

import io.activej.serializer.annotations.SerializeRecord;

@SerializeRecord
public record RpcRequest(int id) {}
