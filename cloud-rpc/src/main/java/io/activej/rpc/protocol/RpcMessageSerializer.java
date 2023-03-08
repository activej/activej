package io.activej.rpc.protocol;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.builder.AbstractBuilder;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.toLinkedHashMap;

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

	public record Entry(Class<?> key, byte index, BinarySerializer<?> serializer, Entry next) {}

	private final BinarySerializer<?>[] serializers;
	private final Entry[] serializersMap;

	public RpcMessageSerializer(LinkedHashMap<Class<?>, BinarySerializer<?>> serializersMap) {
		checkArgument(serializersMap.size() < Byte.MAX_VALUE, "Too many subclasses");

		{
			this.serializers = new BinarySerializer[serializersMap.size()];
			int n = 0;
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				this.serializers[n++] = entry.getValue();
			}
		}
		{
			Entry[] map = new Entry[nextPowerOf2((serializersMap.size() + 3) * 3 / 2)];
			byte n = 0;
			for (Map.Entry<Class<?>, BinarySerializer<?>> entry : serializersMap.entrySet()) {
				Class<?> key = entry.getKey();
				put(map, key, n++, entry.getValue());
			}
			this.serializersMap = map;
		}
	}

	@Deprecated
	public static BinarySerializer<RpcMessage> ofV5(List<Class<?>> messages) {
		return SerializerFactory.builder()
				.withSubclasses(RpcMessage.SUBCLASSES_ID, messages)
				.build()
				.create(DefiningClassLoader.create(), RpcMessage.class);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static RpcMessageSerializer of(Class<?>... messageTypes) {
		return builder().withMessageTypes(messageTypes).build();
	}

	public static RpcMessageSerializer of(List<Class<?>> messageTypes) {
		return builder().withMessageTypes(messageTypes).build();
	}

	public static final class Builder extends AbstractBuilder<Builder, RpcMessageSerializer> {
		private DefiningClassLoader classLoader;
		private SerializerFactory serializerFactory;
		private final LinkedHashMap<Class<?>, BinarySerializer<?>> messageSerializers = new LinkedHashMap<>();

		private Builder() {
			this.messageSerializers.put(RpcControlMessage.class, RpcMessageSerializer.RPC_CONTROL_MESSAGE_SERIALIZER);
			this.messageSerializers.put(RpcRemoteException.class, RpcMessageSerializer.RPC_REMOTE_EXCEPTION_SERIALIZER);
		}

		private DefiningClassLoader ensureClassLoader() {
			if (classLoader == null) {
				classLoader = DefiningClassLoader.create();
			}
			return classLoader;
		}

		private SerializerFactory ensureSerializerFactory() {
			if (serializerFactory == null) {
				serializerFactory = SerializerFactory.defaultInstance();
			}
			return serializerFactory;
		}

		public Builder withClassLoader(DefiningClassLoader classLoader) {
			this.classLoader = classLoader;
			return this;
		}

		public Builder withSerializerFactory(SerializerFactory serializerFactory) {
			this.serializerFactory = serializerFactory;
			return this;
		}

		public Builder with(Class<?> messageType, BinarySerializer<?> serializer) {
			this.messageSerializers.put(messageType, serializer);
			return this;
		}

		public Builder withMessageTypes(Class<?>... messageTypes) {
			return with(ensureClassLoader(), ensureSerializerFactory(), messageTypes);
		}

		/**
		 * @see #with(DefiningClassLoader, SerializerFactory, List)
		 */
		public Builder withMessageTypes(List<Class<?>> messageTypes) {
			return with(ensureClassLoader(), ensureSerializerFactory(), messageTypes);
		}

		/**
		 * @see #with(DefiningClassLoader, SerializerFactory, List)
		 */
		public Builder with(
				DefiningClassLoader classLoader,
				SerializerFactory serializerFactory,
				Class<?>... messageTypes
		) {
			return with(classLoader, serializerFactory, List.of(messageTypes));
		}

		/**
		 * Adds an ability to serialize specified message types.
		 * <p>
		 * <b>
		 * Note: order of added message types matters. It should match an order on the {@link RpcServer}
		 * <p>
		 * Message types should be serializable by passed {@link SerializerFactory}
		 * </b>
		 *
		 * @param classLoader       defining class loader used for creating instances of serializers for specified
		 *                          message types
		 * @param serializerFactory serializer factory used for creating instances of serializers for specified message
		 *                          types
		 * @param messageTypes      classes of messages serialized by the client
		 * @return the builder for RPC client instance capable of serializing provided
		 * message types
		 */
		public Builder with(
				DefiningClassLoader classLoader,
				SerializerFactory serializerFactory,
				List<Class<?>> messageTypes
		) {
			this.messageSerializers.putAll(messageTypes.stream()
					.collect(toLinkedHashMap(messageType -> serializerFactory.create(classLoader, messageType))));
			return this;
		}

		@Override
		protected RpcMessageSerializer doBuild() {
			return new RpcMessageSerializer(this.messageSerializers);
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
		return key.hashCode() & mapLength - 1;
	}

	@Override
	public void encode(BinaryOutput out, RpcMessage item) {
		out.writeInt(item.getIndex());
		Object data = item.getMessage();
		Class<?> key = data == null ? Void.class : data.getClass();
		Entry entry = get(serializersMap, key);
		out.writeByte(entry.index);
		//noinspection unchecked
		((BinarySerializer<Object>) entry.serializer).encode(out, data);
	}

	@Override
	public RpcMessage decode(BinaryInput in) throws CorruptedDataException {
		int index = in.readInt();
		int subClassId = in.readByte();
		Object data = serializers[subClassId].decode(in);
		return new RpcMessage(index, data);
	}
}
