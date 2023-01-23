package io.activej.rpc.server;

import io.activej.rpc.protocol.RpcControlMessage;
import io.activej.rpc.protocol.RpcException;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcRemoteException;
import io.activej.serializer.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.System.identityHashCode;

public class RpcMessageSerializer implements BinarySerializer<RpcMessage> {
	public final RpcMessage rpcMessage = new RpcMessage();

	private record Entry(int key, int index, BinarySerializer<?> serializer, Entry next) {}

	private final BinarySerializer<?>[] serializers;
	private final Entry[] serializersMap;

	public RpcMessageSerializer(LinkedHashMap<Class<?>, BinarySerializer<?>> serializersMap) {
		BinarySerializer<RpcControlMessage> rpcControlMessageBinarySerializer = BinarySerializers.ofEnum(RpcControlMessage.class);
		BinarySerializer<RpcRemoteException> rpcRemoteExceptionBinarySerializer = new BinarySerializer<RpcRemoteException>() {
			@Override
			public void encode(BinaryOutput out, RpcRemoteException item) {
				out.writeUTF8(item.getMessage());
				out.writeUTF8Nullable(item.getCauseMessage());
				out.writeUTF8Nullable(item.getCauseClassName());
			}

			@Override
			public RpcRemoteException decode(BinaryInput in) throws CorruptedDataException {
				return new RpcRemoteException(
						in.readUTF8(),
						in.readUTF8Nullable(),
						in.readUTF8Nullable()
				);
			}
		};
		{
			this.serializers = new BinarySerializer[serializersMap.size() + 2];
			int n = 0;
			this.serializers[n++] = rpcControlMessageBinarySerializer;
			this.serializers[n++] = rpcRemoteExceptionBinarySerializer;
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				this.serializers[n++] = entry.getValue();
			}
		}
		{
			Entry[] map = new Entry[nextPowerOf2((serializersMap.size() + 2) * 3 / 2)];
			int n = -1;
			put(map, identityHashCode(RpcControlMessage.class), n++, rpcControlMessageBinarySerializer);
			put(map, identityHashCode(RpcException.class), n++, rpcRemoteExceptionBinarySerializer);
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				put(map, identityHashCode(entry.getKey()), n++, entry.getValue());
			}
			this.serializersMap = map;
		}
	}

	static int nextPowerOf2(int x) {
		return (-1 >>> Integer.numberOfLeadingZeros(x - 1)) + 1;
	}

	void put(Entry[] map, int key, int index, BinarySerializer<?> serializer) {
		map[key & (map.length - 1)] = new Entry(key, index, serializer, map[key & (map.length - 1)]);
	}

	Entry get(Entry[] map, int key) {
		Entry entry = map[key & (map.length - 1)];
		while (entry != null && entry.key != key) entry = entry.next;
		return entry;
	}

	@Override
	public void encode(BinaryOutput out, RpcMessage item) {
		out.writeInt(item.cookie);
		Object data = item.data;
		Entry entry = get(serializersMap, identityHashCode(data.getClass()));
		out.writeVarInt(entry.index);
		//noinspection unchecked
		((BinarySerializer<Object>) entry.serializer).encode(out, data);
	}

	@Override
	public RpcMessage decode(BinaryInput in) throws CorruptedDataException {
		rpcMessage.cookie = in.readInt();
		int subClassId = in.readVarInt();
		rpcMessage.data = serializers[subClassId + 1].decode(in);
		return rpcMessage;
	}
}
