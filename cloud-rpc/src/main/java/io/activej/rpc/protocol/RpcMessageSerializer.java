package io.activej.rpc.protocol;

import io.activej.common.Checks;
import io.activej.serializer.*;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

public final class RpcMessageSerializer implements BinarySerializer<RpcMessage> {
	private static final boolean CHECK = Checks.isEnabled(RpcMessageSerializer.class);

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

	public record Entry(Class<?> key, byte index, BinarySerializer<?> serializer, Entry next) {}

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
			put(map, RpcControlMessage.class, n++, RPC_CONTROL_MESSAGE_SERIALIZER);
			put(map, Void.class, n++, NULL_SERIALIZER);
			put(map, RpcRemoteException.class, n++, RPC_REMOTE_EXCEPTION_SERIALIZER);
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				Class<?> key = entry.getKey();
				checkArgument(key != Void.class, "Void message type is not supported");
				put(map, key, n++, entry.getValue());
			}
			this.serializersMap = map;
		}
	}

	static int nextPowerOf2(int x) {
		return (-1 >>> Integer.numberOfLeadingZeros(x - 1)) + 1;
	}

	void put(Entry[] map, Class<?> key, byte index, BinarySerializer<?> serializer) {
		int mapIdx = getMapIdx(map.length, key);
		map[mapIdx] = new Entry(key, index, serializer, map[mapIdx]);
	}

	Entry get(Entry[] map, Class<?> key) {
		int mapIdx = getMapIdx(map.length, key);
		Entry entry = map[mapIdx];
		while (entry != null && entry.key != key) entry = entry.next;
		return entry;
	}

	private static int getMapIdx(int mapLength, Class<?> key) {
		return key == Void.class ? 0 : key.hashCode() & mapLength - 1;
	}

	@Override
	public void encode(BinaryOutput out, RpcMessage item) {
		out.writeInt(item.getCookie());
		Object data = item.getData();
		Class<?> key = data == null ? Void.class : data.getClass();
		Entry entry = get(serializersMap, key);
		if (CHECK) {
			checkState(entry != null, "No serializer for RPC message of type: " + key.getName());
		}
		out.writeByte(entry.index);
		//noinspection unchecked
		((BinarySerializer<Object>) entry.serializer).encode(out, data);
	}

	@Override
	public RpcMessage decode(BinaryInput in) throws CorruptedDataException {
		int cookie = in.readInt();
		int subClassId = in.readByte();
		if (CHECK) {
			checkState(subClassId < serializers.length - 1,
					"No serializer for RPC message with subclass index: " + subClassId);
		}
		Object data = serializers[subClassId + 1].decode(in);
		return RpcMessage.of(cookie, data);
	}
}
