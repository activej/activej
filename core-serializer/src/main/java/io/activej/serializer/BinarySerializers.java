/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.serializer;

import io.activej.serializer.util.BinaryOutputUtils;
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
	public static final BinarySerializer<Byte> BYTE_SERIALIZER = new BinarySerializer<>() {
		@Override
		public int encode(byte[] array, int pos, Byte item) {
			return BinaryOutputUtils.writeByte(array, pos, item);
		}

		@Override
		public Byte decode(byte[] array, int pos) {
			return array[pos];
		}

		@Override
		public void encode(BinaryOutput out, Byte item) throws ArrayIndexOutOfBoundsException {
			out.writeByte(item);
		}

		@Override
		public Byte decode(BinaryInput in) {
			return in.readByte();
		}
	};

	/**
	 * A {@link BinarySerializer} implementation for {@link Integer} values.
	 */
	public static final BinarySerializer<Integer> INT_SERIALIZER = new BinarySerializer<>() {
		@Override
		public int encode(byte[] array, int pos, Integer item) {
			array[pos] = (byte) (item >>> 24);
			array[pos + 1] = (byte) (item >>> 16);
			array[pos + 2] = (byte) (item >>> 8);
			array[pos + 3] = (byte) (int) item;
			return pos + 4;
		}

		@Override
		public Integer decode(byte[] array, int pos) {
			return
				(array[pos] & 0xFF) << 24 |
				(array[pos + 1] & 0xFF) << 16 |
				(array[pos + 2] & 0xFF) << 8 |
				array[pos + 3] & 0xFF;
		}

		@Override
		public void encode(BinaryOutput out, Integer item) throws ArrayIndexOutOfBoundsException {
			out.writeInt(item);
		}

		@Override
		public Integer decode(BinaryInput in) {
			return in.readInt();
		}
	};

	/**
	 * A {@link BinarySerializer} implementation for {@link Long} values.
	 */
	public static final BinarySerializer<Long> LONG_SERIALIZER = new BinarySerializer<>() {
		@Override
		public int encode(byte[] array, int pos, Long item) {
			int high = (int) (item >>> 32);
			int low = (int) (long) item;
			array[pos] = (byte) (high >>> 24);
			array[pos + 1] = (byte) (high >>> 16);
			array[pos + 2] = (byte) (high >>> 8);
			array[pos + 3] = (byte) high;
			array[pos + 4] = (byte) (low >>> 24);
			array[pos + 5] = (byte) (low >>> 16);
			array[pos + 6] = (byte) (low >>> 8);
			array[pos + 7] = (byte) low;
			return pos + 8;
		}

		@Override
		public Long decode(byte[] array, int pos) {
			return
				(long) array[pos] << 56 |
				(long) (array[pos + 1] & 0xFF) << 48 |
				(long) (array[pos + 2] & 0xFF) << 40 |
				(long) (array[pos + 3] & 0xFF) << 32 |
				(long) (array[pos + 4] & 0xFF) << 24 |
				(long) ((array[pos + 5] & 0xFF) << 16) |
				(long) ((array[pos + 6] & 0xFF) << 8) |
				(long) (array[pos + 7] & 0xFF);
		}

		@Override
		public void encode(BinaryOutput out, Long item) throws ArrayIndexOutOfBoundsException {
			out.writeLong(item);
		}

		@Override
		public Long decode(BinaryInput in) {
			return in.readLong();
		}
	};

	/**
	 * A {@link BinarySerializer} implementation for {@link Float} values.
	 */
	public static final BinarySerializer<Float> FLOAT_SERIALIZER = new BinarySerializer<>() {
		@Override
		public int encode(byte[] array, int pos, Float item) {
			int v = Float.floatToIntBits(item);
			array[pos] = (byte) (v >>> 24);
			array[pos + 1] = (byte) (v >>> 16);
			array[pos + 2] = (byte) (v >>> 8);
			array[pos + 3] = (byte) v;
			return pos + 4;
		}

		@Override
		public Float decode(byte[] array, int pos) {
			return Float.intBitsToFloat(
				((array[pos] & 0xFF) << 24) |
				((array[pos + 1] & 0xFF) << 16) |
				((array[pos + 2] & 0xFF) << 8) |
				(array[pos + 3] & 0xFF));
		}

		@Override
		public void encode(BinaryOutput out, Float item) throws ArrayIndexOutOfBoundsException {
			out.writeFloat(item);
		}

		@Override
		public Float decode(BinaryInput in) {
			return in.readFloat();
		}
	};

	/**
	 * A {@link BinarySerializer} implementation for {@link Double} values.
	 */
	public static final BinarySerializer<Double> DOUBLE_SERIALIZER = new BinarySerializer<>() {
		@Override
		public int encode(byte[] array, int pos, Double item) {
			long value = Double.doubleToLongBits(item);
			int high = (int) (value >>> 32);
			int low = (int) value;
			array[pos] = (byte) (high >>> 24);
			array[pos + 1] = (byte) (high >>> 16);
			array[pos + 2] = (byte) (high >>> 8);
			array[pos + 3] = (byte) high;
			array[pos + 4] = (byte) (low >>> 24);
			array[pos + 5] = (byte) (low >>> 16);
			array[pos + 6] = (byte) (low >>> 8);
			array[pos + 7] = (byte) low;
			return 8;
		}

		@Override
		public Double decode(byte[] array, int pos) {
			return Double.longBitsToDouble(
				(long) array[pos] << 56 |
				(long) (array[pos + 1] & 0xFF) << 48 |
				(long) (array[pos + 2] & 0xFF) << 40 |
				(long) (array[pos + 3] & 0xFF) << 32 |
				(long) (array[pos + 4] & 0xFF) << 24 |
				(long) (array[pos + 5] & 0xFF) << 16 |
				(long) (array[pos + 6] & 0xFF) << 8 |
				(long) (array[pos + 7] & 0xFF));
		}

		@Override
		public void encode(BinaryOutput out, Double item) throws ArrayIndexOutOfBoundsException {
			out.writeDouble(item);
		}

		@Override
		public Double decode(BinaryInput in) {
			return in.readDouble();
		}
	};

	/**
	 * A {@link BinarySerializer} implementation for UTF-8 encoded {@link String} values.
	 */
	public static final BinarySerializer<String> UTF8_SERIALIZER = new BinarySerializer<>() {
		@Override
		public void encode(BinaryOutput out, String item) throws ArrayIndexOutOfBoundsException {
			out.writeUTF8(item);
		}

		@Override
		public String decode(BinaryInput in) {
			return in.readUTF8();
		}
	};

	/**
	 * A {@link BinarySerializer} implementation for ISO-8859-1 encoded {@link String} values.
	 */
	public static final BinarySerializer<String> ISO_88591_SERIALIZER = new BinarySerializer<>() {
		@Override
		public void encode(BinaryOutput out, String item) throws ArrayIndexOutOfBoundsException {
			out.writeIso88591(item);
		}

		@Override
		public String decode(BinaryInput in) {
			return in.readIso88591();
		}
	};

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
			return (BinarySerializer<T>) new BinarySerializer<String>() {
				@Override
				public void encode(BinaryOutput out, String item) {
					out.writeUTF8Nullable(item);
				}

				@Override
				public String decode(BinaryInput in) {
					return in.readUTF8Nullable();
				}
			};
		}
		if (codec == ISO_88591_SERIALIZER) {
			return (BinarySerializer<T>) new BinarySerializer<String>() {
				@Override
				public void encode(BinaryOutput out, String item) {
					out.writeIso88591Nullable(item);
				}

				@Override
				public String decode(BinaryInput in) {
					return in.readIso88591Nullable();
				}
			};
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
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, E item) {
				out.writeVarInt(item.ordinal());
			}

			@Override
			public E decode(BinaryInput in) throws CorruptedDataException {
				return enumType.getEnumConstants()[in.readVarInt()];
			}
		};
	}

}
