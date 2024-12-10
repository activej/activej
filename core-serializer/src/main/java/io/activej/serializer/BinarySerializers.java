package io.activej.serializer;

import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Supplier;

/**
 * A utility class providing {@link BinarySerializer} instances for types,
 * as well as factory methods for creating serializers for optional values, nullable values,
 * collections, maps, and enums.
 */
public final class BinarySerializers {

	/**
	 * A {@link BinarySerializer} implementation for {@link Byte} values.
	 */
	public static final BinarySerializer<Byte> BYTE_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeByte,
		BinaryInput::readByte
	);

	/**
	 * A {@link BinarySerializer} implementation for {@link Integer} values.
	 */
	public static final BinarySerializer<Integer> INT_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeInt,
		BinaryInput::readInt
	);

	/**
	 * A {@link BinarySerializer} implementation for {@link Long} values.
	 */
	public static final BinarySerializer<Long> LONG_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeLong,
		BinaryInput::readLong
	);

	/**
	 * A {@link BinarySerializer} implementation for {@link Float} values.
	 */
	public static final BinarySerializer<Float> FLOAT_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeFloat,
		BinaryInput::readFloat
	);

	/**
	 * A {@link BinarySerializer} implementation for {@link Double} values.
	 */
	public static final BinarySerializer<Double> DOUBLE_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeDouble,
		BinaryInput::readDouble
	);

	/**
	 * A {@link BinarySerializer} implementation for UTF-8 encoded {@link String} values.
	 */
	public static final BinarySerializer<String> UTF8_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeUTF8,
		BinaryInput::readUTF8
	);

	/**
	 * A {@link BinarySerializer} implementation for ISO-8859-1 encoded {@link String} values.
	 */
	public static final BinarySerializer<String> ISO_88591_SERIALIZER = new SimpleBinarySerializer<>(
		BinaryOutput::writeIso88591,
		BinaryInput::readIso88591
	);

	/**
	 * A {@link BinarySerializer} implementation for {@code byte[]} arrays.
	 */
	public static final BinarySerializer<byte[]> BYTES_SERIALIZER = new BinarySerializer<>() {
		@Override
		public void encode(BinaryOutput out, byte[] item) throws ArrayIndexOutOfBoundsException {
			out.writeVarInt(item.length);
			out.write(item);
		}

		@Override
		public byte[] decode(BinaryInput in) {
			int size = in.readVarInt();
			byte[] bytes = new byte[size];
			in.read(bytes);
			return bytes;
		}
	};

	/**
	 * Returns a {@link BinarySerializer} for {@link Optional} values of the given type.
	 *
	 * @param codec the {@link BinarySerializer} for the optional value type
	 * @param <T> the type of the optional value
	 * @return a {@link BinarySerializer} for {@link Optional} values
	 */
	public static <T> BinarySerializer<Optional<T>> ofOptional(BinarySerializer<T> codec) {
		return new BinarySerializer<>() {
			final BinarySerializer<T> nullable = ofNullable(codec);

			@Override
			public void encode(BinaryOutput out, Optional<T> item) {
				nullable.encode(out, item.orElse(null));
			}

			@Override
			public Optional<T> decode(BinaryInput in) {
				return Optional.ofNullable(nullable.decode(in));
			}
		};
	}

	/**
	 * Returns a {@link BinarySerializer} for nullable values of the given type.
	 *
	 * @param codec the {@link BinarySerializer} for the value type
	 * @param <T> the type of the value
	 * @return a {@link BinarySerializer} for nullable values
	 */
	@SuppressWarnings("unchecked")
	public static <T> BinarySerializer<@Nullable T> ofNullable(BinarySerializer<T> codec) {
		if (codec == UTF8_SERIALIZER) {
			return (BinarySerializer<T>) new SimpleNullableBinarySerializer<>(
				BinaryOutput::writeUTF8Nullable,
				BinaryInput::readUTF8Nullable
			);
		}
		if (codec == ISO_88591_SERIALIZER) {
			return (BinarySerializer<T>) new SimpleNullableBinarySerializer<>(
				BinaryOutput::writeIso88591Nullable,
				BinaryInput::readIso88591Nullable
			);
		}
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, T item) {
				if (item != null) {
					out.writeBoolean(true);
					codec.encode(out, item);
				} else {
					out.writeBoolean(false);
				}
			}

			@Override
			public T decode(BinaryInput in) {
				if (in.readBoolean()) {
					return codec.decode(in);
				} else {
					return null;
				}
			}
		};
	}

	/**
	 * Returns a {@link BinarySerializer} for a {@link Collection} of the given element type.
	 *
	 * @param element the {@link BinarySerializer} for the element type
	 * @param constructor a supplier that creates an empty collection
	 * @param <E> the type of the elements
	 * @param <C> the type of the collection
	 * @return a {@link BinarySerializer} for a collection of elements
	 */
	private static <E, C extends Collection<E>> BinarySerializer<C> ofCollection(BinarySerializer<E> element, Supplier<C> constructor) {
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, C item) {
				out.writeVarInt(item.size());
				for (E v : item) {
					element.encode(out, v);
				}
			}

			@Override
			public C decode(BinaryInput in) {
				C collection = constructor.get();
				int size = in.readVarInt();
				for (int i = 0; i < size; i++) {
					collection.add(element.decode(in));
				}
				return collection;
			}
		};
	}

	/**
	 * Returns a {@link BinarySerializer} for a {@link List} of the given element type.
	 *
	 * @param element the {@link BinarySerializer} for the list element type
	 * @param <E> the type of the list elements
	 * @return a {@link BinarySerializer} for a list of elements
	 */
	public static <E> BinarySerializer<List<E>> ofList(BinarySerializer<E> element) {
		return ofCollection(element, ArrayList::new);
	}

	/**
	 * Returns a {@link BinarySerializer} for a {@link Set} of the given element type.
	 *
	 * @param element the {@link BinarySerializer} for the set element type
	 * @param <E> the type of the set elements
	 * @return a {@link BinarySerializer} for a set of elements
	 */
	public static <E> BinarySerializer<Set<E>> ofSet(BinarySerializer<E> element) {
		return ofCollection(element, LinkedHashSet::new);
	}

	/**
	 * Returns a {@link BinarySerializer} for a {@link Map} with keys and values of the given types.
	 *
	 * @param key the {@link BinarySerializer} for the key type
	 * @param value the {@link BinarySerializer} for the value type
	 * @param <K> the type of the map keys
	 * @param <V> the type of the map values
	 * @return a {@link BinarySerializer} for a map of key-value pairs
	 */
	public static <K, V> BinarySerializer<Map<K, V>> ofMap(BinarySerializer<K> key, BinarySerializer<V> value) {
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, Map<K, V> item) {
				out.writeVarInt(item.size());
				for (Map.Entry<K, V> entry : item.entrySet()) {
					key.encode(out, entry.getKey());
					value.encode(out, entry.getValue());
				}
			}

			@Override
			public Map<K, V> decode(BinaryInput in) {
				Map<K, V> map = new LinkedHashMap<>();
				int size = in.readVarInt();
				for (int i = 0; i < size; i++) {
					map.put(key.decode(in), value.decode(in));
				}
				return map;
			}
		};
	}

	/**
	 * Returns a {@link BinarySerializer} for the given {@link Enum} type.
	 *
	 * @param enumType the class of the enum type
	 * @param <E> the enum type
	 * @return a {@link BinarySerializer} for the enum type
	 */
	public static <E extends Enum<E>> BinarySerializer<E> ofEnum(Class<E> enumType) {
		E[] constants = enumType.getEnumConstants();
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, E item) {
				out.writeVarInt(item.ordinal());
			}

			@Override
			public E decode(BinaryInput in) throws CorruptedDataException {
				int ordinal = in.readVarInt();
				if (ordinal < 0 || ordinal >= constants.length) {
					throw new CorruptedDataException("Invalid enum ordinal: " + ordinal);
				}
				return constants[ordinal];
			}
		};
	}

	// Helper classes to reduce redundancy
	private static class SimpleBinarySerializer<T> implements BinarySerializer<T> {
		private final Encoder<T> encoder;
		private final Decoder<T> decoder;

		SimpleBinarySerializer(Encoder<T> encoder, Decoder<T> decoder) {
			this.encoder = encoder;
			this.decoder = decoder;
		}

		@Override
		public void encode(BinaryOutput out, T item) throws ArrayIndexOutOfBoundsException {
			encoder.encode(out, item);
		}

		@Override
		public T decode(BinaryInput in) {
			return decoder.decode(in);
		}
	}

	private static class SimpleNullableBinarySerializer<T> implements BinarySerializer<@Nullable T> {
		private final Encoder<@Nullable T> encoder;
		private final Decoder<@Nullable T> decoder;

		SimpleNullableBinarySerializer(Encoder<@Nullable T> encoder, Decoder<@Nullable T> decoder) {
			this.encoder = encoder;
			this.decoder = decoder;
		}

		@Override
		public void encode(BinaryOutput out, @Nullable T item) throws ArrayIndexOutOfBoundsException {
			encoder.encode(out, item);
		}

		@Override
		public @Nullable T decode(BinaryInput in) {
			return decoder.decode(in);
		}
	}

	@FunctionalInterface
	private interface Encoder<T> {
		void encode(BinaryOutput out, T item) throws ArrayIndexOutOfBoundsException;
	}

	@FunctionalInterface
	private interface Decoder<T> {
		T decode(BinaryInput in);
	}

}
