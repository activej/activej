package io.activej.dataflow.codec;

import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.messaging.Version;
import io.activej.serializer.CorruptedDataException;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;
import io.activej.streamcodecs.StructuredStreamCodec;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class Utils {
	private static final Map<Integer, Class<?>> INDEX_TO_CLASS = new HashMap<>();
	private static final Map<Class<?>, Integer> CLASS_TO_INDEX = new HashMap<>();

	public static final StreamCodec<StreamId> STREAM_ID_STREAM_CODEC = StructuredStreamCodec.create(StreamId::new,
			StreamId::getId, StreamCodecs.ofVarLong()
	);
	public static final StreamCodec<Version> VERSION_STREAM_CODEC = StructuredStreamCodec.create(Version::new,
			Version::major, StreamCodecs.ofVarInt(),
			Version::minor, StreamCodecs.ofVarInt()
	);
	public static final StreamCodec<Instant> INSTANT_STREAM_CODEC = StructuredStreamCodec.create(Instant::ofEpochSecond,
			Instant::getEpochSecond, StreamCodecs.ofVarLong(),
			Instant::getNano, StreamCodecs.ofVarInt());
	public static final StreamCodec<Class<?>> CLASS_STREAM_CODEC = StreamCodec.of(
			(output, item) -> {
				Integer index = CLASS_TO_INDEX.getOrDefault(item, 0);
				output.writeByte(index.byteValue());
				if (index == 0) {
					output.writeString(item.getName());
				}
			},
			input -> {
				byte index = input.readByte();
				Class<?> cls = INDEX_TO_CLASS.get(((int) index));
				if (cls != null) return cls;

				try {
					return Class.forName(input.readString());
				} catch (ClassNotFoundException e) {
					throw new CorruptedDataException(e.getMessage());
				}
			}
	);

	static {
		addClassToCache(byte.class);
		addClassToCache(short.class);
		addClassToCache(int.class);
		addClassToCache(long.class);
		addClassToCache(float.class);
		addClassToCache(double.class);
		addClassToCache(char.class);
		addClassToCache(boolean.class);
		addClassToCache(String.class);
	}

	private static void addClassToCache(Class<?> cls) {
		int nextIdx = CLASS_TO_INDEX.size() + 1;

		CLASS_TO_INDEX.put(cls, nextIdx);
		INDEX_TO_CLASS.put(nextIdx, cls);
	}
}
