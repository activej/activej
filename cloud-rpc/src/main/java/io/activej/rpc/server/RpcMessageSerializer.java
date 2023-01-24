package io.activej.rpc.server;

import io.activej.rpc.protocol.RpcControlMessage;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcRemoteException;
import io.activej.serializer.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;
import static java.lang.System.identityHashCode;

public final class RpcMessageSerializer implements BinarySerializer<RpcMessage> {

	private static final BinarySerializer<RpcRemoteException> RPC_REMOTE_EXCEPTION_SERIALIZER = new BinarySerializer<>() {
		@Override
		public void encode(BinaryOutput out, RpcRemoteException item) {
			out.writeUTF8Nullable(item.getCauseClassName());
			out.writeUTF8Nullable(item.getCauseMessage());
			out.writeUTF8Nullable(item.getMessage());
		}

		@Override
		public RpcRemoteException decode(BinaryInput in) throws CorruptedDataException {
			String causeClassName = in.readUTF8Nullable();
			String causeMessage = in.readUTF8Nullable();
			String message = in.readUTF8Nullable();
			return new RpcRemoteException(
					message,
					causeClassName,
					causeMessage
			);
		}
	};
	private static final BinarySerializer<RpcControlMessage> RPC_CONTROL_MESSAGE_SERIALIZER = BinarySerializers.ofEnum(RpcControlMessage.class);
	private static final BinarySerializer<Object> NULL_SERIALIZER = new BinarySerializer<>() {
		@Override
		public void encode(BinaryOutput out, Object item) {
		}

		@Override
		public Object decode(BinaryInput in) throws CorruptedDataException {
			return null;
		}
	};

	private record Entry(int key, byte index, BinarySerializer<?> serializer, Entry next) {}

	private final BinarySerializer<?>[] serializers;
	private final Entry[] serializersMap;

	public RpcMessageSerializer(LinkedHashMap<Class<?>, BinarySerializer<?>> serializersMap) {
		checkArgument(serializersMap.size() < Byte.MAX_VALUE - 3, "Too many subclasses");

		{
			this.serializers = new BinarySerializer[serializersMap.size() + 3];
			int n = 0;
			this.serializers[n++] = RPC_CONTROL_MESSAGE_SERIALIZER;
			this.serializers[n++] = NULL_SERIALIZER;
			this.serializers[n++] = RPC_REMOTE_EXCEPTION_SERIALIZER;
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				this.serializers[n++] = entry.getValue();
			}
		}
		{
			Entry[] map = new Entry[nextPowerOf2((serializersMap.size() + 3) * 3 / 2)];
			byte n = -1;
			put(map, identityHashCode(RpcControlMessage.class), n++, RPC_CONTROL_MESSAGE_SERIALIZER);
			put(map, 0, n++, NULL_SERIALIZER);
			put(map, identityHashCode(RpcRemoteException.class), n++, RPC_REMOTE_EXCEPTION_SERIALIZER);
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				put(map, identityHashCode(entry.getKey()), n++, entry.getValue());
			}
			this.serializersMap = map;
		}
	}

	static int nextPowerOf2(int x) {
		return (-1 >>> Integer.numberOfLeadingZeros(x - 1)) + 1;
	}

	void put(Entry[] map, int key, byte index, BinarySerializer<?> serializer) {
		map[key & (map.length - 1)] = new Entry(key, index, serializer, map[key & (map.length - 1)]);
	}

	Entry get(Entry[] map, int key) {
		Entry entry = map[key & (map.length - 1)];
		while (entry != null && entry.key != key) entry = entry.next;
		return entry;
	}

	@Override
	public void encode(BinaryOutput out, RpcMessage item) {
		out.writeInt(item.getCookie());
		Object data = item.getData();
		int key = data == null ? 0 : identityHashCode(data.getClass());
		Entry entry = get(serializersMap, key);
		out.writeByte(entry.index);
		//noinspection unchecked
		((BinarySerializer<Object>) entry.serializer).encode(out, data);
	}

	@Override
	public RpcMessage decode(BinaryInput in) throws CorruptedDataException {
		int cookie = in.readInt();
		int subClassId = in.readByte();
		Object data = serializers[subClassId + 1].decode(in);
		return RpcMessage.of(cookie, data);
	}
}
