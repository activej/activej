package io.activej.serializer.datastream;

import io.activej.serializer.SerializeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public final class DataStreamCodecs {
	public static final Map<Class<?>, DataStreamCodec<?>> REGISTRY;

	static {
		Map<Class<?>, DataStreamCodec<?>> map = new HashMap<>();
		map.put(boolean.class, ofBoolean());
		map.put(char.class, ofChar());
		map.put(byte.class, ofByte());
		map.put(int.class, ofVarInt());
		map.put(short.class, ofShort());
		map.put(long.class, ofVarLong());
		map.put(float.class, ofFloat());
		map.put(double.class, ofDouble());
		map.put(Boolean.class, ofNullable(ofBoolean()));
		map.put(Character.class, ofNullable(ofChar()));
		map.put(Byte.class, ofNullable(ofByte()));
		map.put(Integer.class, ofNullable(ofInt()));
		map.put(Short.class, ofNullable(ofShort()));
		map.put(Long.class, ofNullable(ofLong()));
		map.put(Float.class, ofNullable(ofFloat()));
		map.put(Double.class, ofNullable(ofDouble()));
		map.put(String.class, ofString());
		REGISTRY = unmodifiableMap(map);
	}

	public static DataStreamCodec<Boolean> ofBoolean() {
		return new DataStreamCodec<Boolean>() {
			@Override
			public Boolean decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readBoolean();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Boolean item) throws IOException, SerializeException {
				stream.writeBoolean(item);
			}
		};
	}

	public static DataStreamCodec<Character> ofChar() {
		return new DataStreamCodec<Character>() {
			@Override
			public Character decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readChar();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Character item) throws IOException, SerializeException {
				stream.writeChar(item);
			}
		};
	}

	public static DataStreamCodec<Byte> ofByte() {
		return new DataStreamCodec<Byte>() {
			@Override
			public Byte decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readByte();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Byte item) throws IOException, SerializeException {
				stream.writeByte(item);
			}
		};
	}

	public static DataStreamCodec<Short> ofShort() {
		return new DataStreamCodec<Short>() {
			@Override
			public Short decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readShort();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Short item) throws IOException, SerializeException {
				stream.writeShort(item);
			}
		};
	}

	public static DataStreamCodec<Integer> ofInt() {
		return new DataStreamCodec<Integer>() {
			@Override
			public Integer decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readInt();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Integer item) throws IOException, SerializeException {
				stream.writeInt(item);
			}
		};
	}

	public static DataStreamCodec<Integer> ofVarInt() {
		return new DataStreamCodec<Integer>() {
			@Override
			public Integer decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readVarInt();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Integer item) throws IOException, SerializeException {
				stream.writeVarInt(item);
			}
		};
	}

	public static DataStreamCodec<Long> ofLong() {
		return new DataStreamCodec<Long>() {
			@Override
			public Long decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readLong();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Long item) throws IOException, SerializeException {
				stream.writeLong(item);
			}
		};
	}

	public static DataStreamCodec<Long> ofVarLong() {
		return new DataStreamCodec<Long>() {
			@Override
			public Long decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readVarLong();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Long item) throws IOException, SerializeException {
				stream.writeVarLong(item);
			}
		};
	}

	public static DataStreamCodec<Float> ofFloat() {
		return new DataStreamCodec<Float>() {
			@Override
			public Float decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readFloat();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Float item) throws IOException, SerializeException {
				stream.writeFloat(item);
			}
		};
	}

	public static DataStreamCodec<Double> ofDouble() {
		return new DataStreamCodec<Double>() {
			@Override
			public Double decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readDouble();
			}

			@Override
			public void encode(DataOutputStreamEx stream, Double item) throws IOException, SerializeException {
				stream.writeDouble(item);
			}
		};
	}

	public static DataStreamCodec<String> ofString() {
		return new DataStreamCodec<String>() {
			@Override
			public @Nullable String decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				return stream.readString();
			}

			@Override
			public void encode(DataOutputStreamEx stream, @Nullable String item) throws IOException, SerializeException {
				stream.writeString(item);
			}
		};
	}

	public static <T> DataStreamCodec<@Nullable T> ofNullable(DataStreamCodec<@NotNull T> codec) {
		return new DataStreamCodec<T>() {
			@Override
			public @Nullable T decode(DataInputStreamEx stream) throws IOException, DeserializeException {
				if (stream.readByte() == 0) return null;
				return codec.decode(stream);
			}

			@Override
			public void encode(DataOutputStreamEx stream, @Nullable T item) throws IOException, SerializeException {
				if (item == null) {
					stream.writeByte((byte) 0);
				} else {
					stream.writeByte((byte) 1);
					codec.encode(stream, item);
				}
			}
		};
	}

}
