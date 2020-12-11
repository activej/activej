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

package io.activej.codec;

import io.activej.common.api.ParserFunction;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UncheckedException;
import io.activej.common.tuple.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.map;
import static java.util.Arrays.asList;
import static java.util.Collections.*;

/**
 * This class contains various primitive {@link StructuredCodec StructuredCodecs} and their combinators.
 */
@SuppressWarnings("unchecked")
public final class StructuredCodecs {

	public static final StructuredCodec<Boolean> BOOLEAN_CODEC = new StructuredCodec<Boolean>() {
		@Override
		public void encode(StructuredOutput out, Boolean value) {
			out.writeBoolean(value);
		}

		@Override
		public Boolean decode(StructuredInput in) throws MalformedDataException {
			return in.readBoolean();
		}
	};

	public static final StructuredCodec<Character> CHARACTER_CODEC = new StructuredCodec<Character>() {
		@Override
		public void encode(StructuredOutput out, Character value) {
			out.writeString(value + "");
		}

		@Override
		public Character decode(StructuredInput in) throws MalformedDataException {
			String v = in.readString();
			if (v.length() == 1) {
				return v.charAt(0);
			}
			throw new MalformedDataException("Read a string with length != 1 while trying to read a character");
		}
	};

	public static final StructuredCodec<Byte> BYTE_CODEC = new StructuredCodec<Byte>() {
		@Override
		public void encode(StructuredOutput out, Byte value) {
			out.writeInt(value & 0xFF);
		}

		@Override
		public Byte decode(StructuredInput in) throws MalformedDataException {
			int v = in.readInt();
			if (v >= 0 && v <= 0xFF) {
				return (byte) v;
			}
			throw new MalformedDataException("Read an int not in range [0, 255] while trying to read a byte");
		}
	};

	public static final StructuredCodec<Short> SHORT_CODEC = new StructuredCodec<Short>() {
		@Override
		public void encode(StructuredOutput out, Short value) {
			out.writeInt(value);
		}

		@Override
		public Short decode(StructuredInput in) throws MalformedDataException {
			int v = in.readInt();
			if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE) {
				return (short) v;
			}
			throw new MalformedDataException("Read an int not in range [" + Short.MIN_VALUE + ", " + Short.MAX_VALUE + "] while trying to read a short");
		}
	};

	public static final StructuredCodec<Integer> INT_CODEC = new StructuredCodec<Integer>() {
		@Override
		public void encode(StructuredOutput out, Integer value) {
			out.writeInt(value);
		}

		@Override
		public Integer decode(StructuredInput in) throws MalformedDataException {
			return in.readInt();
		}
	};

	public static final StructuredCodec<Long> LONG_CODEC = new StructuredCodec<Long>() {
		@Override
		public void encode(StructuredOutput out, Long value) {
			out.writeLong(value);
		}

		@Override
		public Long decode(StructuredInput in) throws MalformedDataException {
			return in.readLong();
		}
	};

	public static final StructuredCodec<Integer> INT32_CODEC = new StructuredCodec<Integer>() {
		@Override
		public void encode(StructuredOutput out, Integer value) {
			out.writeInt32(value);
		}

		@Override
		public Integer decode(StructuredInput in) throws MalformedDataException {
			return in.readInt32();
		}
	};

	public static final StructuredCodec<Long> LONG64_CODEC = new StructuredCodec<Long>() {
		@Override
		public void encode(StructuredOutput out, Long value) {
			out.writeLong64(value);
		}

		@Override
		public Long decode(StructuredInput in) throws MalformedDataException {
			return in.readLong64();
		}
	};

	public static final StructuredCodec<Float> FLOAT_CODEC = new StructuredCodec<Float>() {
		@Override
		public void encode(StructuredOutput out, Float value) {
			out.writeFloat(value);
		}

		@Override
		public Float decode(StructuredInput in) throws MalformedDataException {
			return in.readFloat();
		}
	};

	public static final StructuredCodec<Double> DOUBLE_CODEC = new StructuredCodec<Double>() {
		@Override
		public void encode(StructuredOutput out, Double value) {
			out.writeDouble(value);
		}

		@Override
		public Double decode(StructuredInput in) throws MalformedDataException {
			return in.readDouble();
		}
	};

	public static final StructuredCodec<String> STRING_CODEC = new StructuredCodec<String>() {
		@Override
		public void encode(StructuredOutput out, String value) {
			out.writeString(value);
		}

		@Override
		public String decode(StructuredInput in) throws MalformedDataException {
			return in.readString();
		}
	};

	public static final StructuredCodec<byte[]> BYTES_CODEC = new StructuredCodec<byte[]>() {
		@Override
		public void encode(StructuredOutput out, byte[] value) {
			out.writeBytes(value);
		}

		@Override
		public byte[] decode(StructuredInput in) throws MalformedDataException {
			return in.readBytes();
		}
	};

	public static final StructuredCodec<Void> VOID_CODEC = new StructuredCodec<Void>() {
		@Override
		public Void decode(StructuredInput in) throws MalformedDataException {
			in.readNull();
			return null;
		}

		@Override
		public void encode(StructuredOutput out, Void item) {
			out.writeNull();
		}
	};

	public static <E extends Enum<E>> StructuredCodec<E> ofEnum(Class<E> enumType) {
		return new StructuredCodec<E>() {
			@Override
			public void encode(StructuredOutput out, E value) {
				out.writeString(value.name());
			}

			@Override
			public E decode(StructuredInput in) throws MalformedDataException {
				return Enum.valueOf(enumType, in.readString());
			}
		};
	}

	public static final StructuredCodec<Class<?>> CLASS_CODEC = new StructuredCodec<Class<?>>() {
		@Override
		public void encode(StructuredOutput out, Class<?> value) {
			out.writeString(value.getName());
		}

		@Override
		public Class<?> decode(StructuredInput in) throws MalformedDataException {
			try {
				return Class.forName(in.readString());
			} catch (ClassNotFoundException e) {
				throw new MalformedDataException(e);
			}
		}
	};

	@SuppressWarnings("rawtypes")
	public static <T> StructuredCodec<Class<? extends T>> ofClass() {
		return (StructuredCodec) CLASS_CODEC;
	}

	static <T> StructuredCodec<T> ofCustomType(Class<T> type) {
		return ofCustomType((Type) type);
	}

	static <T> StructuredCodec<T> ofCustomType(Type type) {
		return new StructuredCodec<T>() {
			@Override
			public void encode(StructuredOutput out, T item) {
				out.writeCustom(type, item);
			}

			@Override
			public T decode(StructuredInput in) throws MalformedDataException {
				return in.readCustom(type);
			}
		};
	}

	/**
	 * Combinator codec that writes/reads Optional&lt;T&gt; as nullable T with given codec for T
	 */
	public static <T> StructuredCodec<Optional<T>> ofOptional(StructuredCodec<T> codec) {
		return new StructuredCodec<Optional<T>>() {
			@Override
			public void encode(StructuredOutput out, Optional<T> item) {
				out.writeNullable(codec, item.orElse(null));
			}

			@Override
			public Optional<T> decode(StructuredInput in) throws MalformedDataException {
				return Optional.ofNullable(in.readNullable(codec));
			}
		};
	}

	public static <T> StructuredCodec<@Nullable T> ofNullable(StructuredCodec<T> codec) {
		return new StructuredCodec<T>() {
			@Override
			public void encode(StructuredOutput out, T item) {
				out.writeNullable(codec, item);
			}

			@Nullable
			@Override
			public T decode(StructuredInput in) throws MalformedDataException {
				return in.readNullable(codec);
			}
		};
	}

	public static <T, R> StructuredCodec<R> transform(StructuredCodec<T> codec, ParserFunction<T, R> reader, Function<R, T> writer) {
		return new StructuredCodec<R>() {
			@Override
			public void encode(StructuredOutput out, R value) {
				T result = writer.apply(value);
				codec.encode(out, result);
			}

			@Override
			public R decode(StructuredInput in) throws MalformedDataException {
				T result = codec.decode(in);
				try {
					return reader.parse(result);
				} catch (UncheckedException u) {
					throw u.propagate(MalformedDataException.class);
				}
			}
		};
	}


	/**
	 * Combinator codec that writes/reads a list of T with given codec for T
	 */
	public static <T> StructuredCodec<List<T>> ofList(StructuredCodec<T> valueAdapters) {
		return new StructuredCodec<List<T>>() {
			@Override
			public void encode(StructuredOutput out, List<T> item) {
				out.writeList(valueAdapters, item);
			}

			@Override
			public List<T> decode(StructuredInput in) throws MalformedDataException {
				return in.readList(valueAdapters);
			}
		};
	}

	/**
	 * Combinator codec that writes/reads a set of T with given codec for T
	 */
	public static <T> StructuredCodec<Set<T>> ofSet(StructuredCodec<T> codec) {
		return ofList(codec)
				.transform(LinkedHashSet::new, ArrayList::new);
	}

	/**
	 * Combinator codec that writes/reads a heterogeneous fixed-size array ob objects with given codecs
	 */
	public static StructuredCodec<Object[]> ofTupleArray(StructuredCodec<?>... elementDecoders) {
		return ofTupleList(asList(elementDecoders))
				.transform(
						list -> list.toArray(new Object[0]),
						Arrays::asList
				);
	}

	public static StructuredCodec<Object[]> ofTupleArray(List<StructuredCodec<?>> codecs) {
		return ofTupleList(codecs)
				.transform(
						list -> list.toArray(new Object[0]),
						Arrays::asList
				);
	}

	public static <T> StructuredCodec<List<T>> ofTupleList(StructuredCodec<? extends T>... elementDecoders) {
		return ofTupleList(asList(elementDecoders));
	}

	public static <T> StructuredCodec<List<T>> ofTupleList(List<StructuredCodec<? extends T>> codecs) {
		return new StructuredCodec<List<T>>() {
			@Override
			public List<T> decode(StructuredInput in) throws MalformedDataException {
				return in.readTuple($ -> {
					List<T> list = new ArrayList<>();
					for (StructuredCodec<? extends T> codec : codecs) {
						list.add(codec.decode(in));
					}
					return list;
				});
			}

			@Override
			public void encode(StructuredOutput out, List<T> list) {
				checkArgument(list.size() == codecs.size());
				Iterator<T> it = list.iterator();
				out.writeTuple(() -> {
					for (StructuredCodec<? extends T> codec : codecs) {
						((StructuredCodec<T>) codec).encode(out, it.next());
					}
				});
			}
		};
	}

	/**
	 * Combinator codec that writes/reads a map with keys of type K and values of type V with given codecs for K and V
	 */
	public static <K, V> StructuredCodec<Map<K, V>> ofMap(StructuredCodec<K> codecKey, StructuredCodec<V> codecValue) {
		return new StructuredCodec<Map<K, V>>() {
			@Override
			public void encode(StructuredOutput out, Map<K, V> map) {
				out.writeMap(codecKey, codecValue, map);
			}

			@Override
			public Map<K, V> decode(StructuredInput in) throws MalformedDataException {
				return in.readMap(codecKey, codecValue);
			}
		};
	}

	/**
	 * Combinator codec that writes/reads a heterogeneous map with string keys and values using codecs from given map
	 */
	public static <T> StructuredCodec<Map<String, T>> ofObjectMap(Map<String, StructuredCodec<? extends T>> fieldCodecs) {
		return new StructuredCodec<Map<String, T>>() {
			@Override
			public Map<String, T> decode(StructuredInput in) throws MalformedDataException {
				return in.readObject($ -> {
					Map<String, T> map = new LinkedHashMap<>();
					for (Map.Entry<String, StructuredCodec<? extends T>> entry : fieldCodecs.entrySet()) {
						String field = entry.getKey();
						in.readKey(field);
						StructuredCodec<? extends T> codec = entry.getValue();
						map.put(field, codec.decode(in));
					}
					return map;
				});
			}

			@Override
			public void encode(StructuredOutput out, Map<String, T> map) {
				out.writeObject(() -> {
					for (Map.Entry<String, StructuredCodec<? extends T>> entry : fieldCodecs.entrySet()) {
						String field = entry.getKey();
						out.writeKey(field);
						StructuredCodec<T> codec = (StructuredCodec<T>) entry.getValue();
						codec.encode(out, map.get(field));
					}
				});
			}
		};
	}

	public static <T> StructuredCodec<List<T>> concat(StructuredCodec<? extends T>... elementCodecs) {
		return concat(asList(elementCodecs));
	}

	/**
	 * Combinator codec that writes/reads a heterogeneous list using codecs from given list without list boundaries
	 */
	public static <T> StructuredCodec<List<T>> concat(List<StructuredCodec<? extends T>> elementCodecs) {
		return new StructuredCodec<List<T>>() {
			@Override
			public List<T> decode(StructuredInput in) throws MalformedDataException {
				List<T> result = new ArrayList<>(elementCodecs.size());
				for (StructuredCodec<? extends T> elementCodec : elementCodecs) {
					result.add(elementCodec.decode(in));
				}
				return result;
			}

			@Override
			public void encode(StructuredOutput out, List<T> item) {
				checkArgument(item.size() == elementCodecs.size());
				for (int i = 0; i < elementCodecs.size(); i++) {
					((StructuredCodec<T>) elementCodecs.get(i)).encode(out, item.get(i));
				}
			}
		};
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R> StructuredCodec<R> tuple(TupleParser0<R> constructor) {
		return ofTupleList()
				.transform(
						list -> constructor.create(),
						item -> emptyList());
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R, T1> StructuredCodec<R> tuple(TupleParser1<T1, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1) {
		return ofTupleList(codec1)
				.transform(
						list -> constructor.create(list.get(0)),
						item -> singletonList(getter1.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R, T1, T2> StructuredCodec<R> tuple(TupleParser2<T1, T2, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2) {
		return ofTupleList(codec1, codec2)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1)),
						item -> asList(getter1.apply(item), getter2.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R, T1, T2, T3> StructuredCodec<R> tuple(TupleParser3<T1, T2, T3, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3) {
		return ofTupleList(codec1, codec2, codec3)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R, T1, T2, T3, T4> StructuredCodec<R> tuple(TupleParser4<T1, T2, T3, T4, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3,
			Function<R, T4> getter4, StructuredCodec<T4> codec4) {
		return ofTupleList(codec1, codec2, codec3, codec4)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item), getter4.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R, T1, T2, T3, T4, T5> StructuredCodec<R> tuple(TupleParser5<T1, T2, T3, T4, T5, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3,
			Function<R, T4> getter4, StructuredCodec<T4> codec4,
			Function<R, T5> getter5, StructuredCodec<T5> codec5) {
		return ofTupleList(codec1, codec2, codec3, codec4, codec5)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item), getter4.apply(item), getter5.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofTupleList(List)} with fixed number of arguments and map it to some result type R
	 */
	public static <R, T1, T2, T3, T4, T5, T6> StructuredCodec<R> tuple(TupleParser6<T1, T2, T3, T4, T5, T6, R> constructor,
			Function<R, T1> getter1, StructuredCodec<T1> codec1,
			Function<R, T2> getter2, StructuredCodec<T2> codec2,
			Function<R, T3> getter3, StructuredCodec<T3> codec3,
			Function<R, T4> getter4, StructuredCodec<T4> codec4,
			Function<R, T5> getter5, StructuredCodec<T5> codec5,
			Function<R, T6> getter6, StructuredCodec<T6> codec6) {
		return ofTupleList(codec1, codec2, codec3, codec4, codec5, codec6)
				.transform(
						list -> constructor.create((T1) list.get(0), (T2) list.get(1), (T3) list.get(2), (T4) list.get(3), (T5) list.get(4), (T6) list.get(5)),
						item -> asList(getter1.apply(item), getter2.apply(item), getter3.apply(item), getter4.apply(item), getter5.apply(item), getter6.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R> StructuredCodec<R> object(TupleParser0<R> constructor) {
		return ofObjectMap(emptyMap())
				.transform(
						map -> constructor.create(),
						item -> map());
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R, T1> StructuredCodec<R> object(TupleParser1<T1, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1) {
		return ofObjectMap(map(field1, codec1))
				.transform(
						map -> constructor.create(map.get(field1)),
						item -> map(field1, getter1.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R, T1, T2> StructuredCodec<R> object(TupleParser2<T1, T2, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2) {
		return ofObjectMap(map(field1, codec1, field2, codec2))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R, T1, T2, T3> StructuredCodec<R> object(TupleParser3<T1, T2, T3, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3) {
		return ofObjectMap(map(field1, codec1, field2, codec2, field3, codec3))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R, T1, T2, T3, T4> StructuredCodec<R> object(TupleParser4<T1, T2, T3, T4, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3,
			String field4, Function<R, T4> getter4, StructuredCodec<T4> codec4) {
		return ofObjectMap(map(field1, codec1, field2, codec2, field3, codec3, field4, codec4))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item), field4, getter4.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R, T1, T2, T3, T4, T5> StructuredCodec<R> object(TupleParser5<T1, T2, T3, T4, T5, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3,
			String field4, Function<R, T4> getter4, StructuredCodec<T4> codec4,
			String field5, Function<R, T5> getter5, StructuredCodec<T5> codec5) {
		return ofObjectMap(map(field1, codec1, field2, codec2, field3, codec3, field4, codec4, field5, codec5))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4), (T5) map.get(field5)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item), field4, getter4.apply(item), field5, getter5.apply(item)));
	}

	/**
	 * A DSL to call {@link #ofObjectMap(Map)} with fixed number of key-value pairs and map it to some result type R.
	 * This is the main combinator for POJO codecs
	 */
	public static <R, T1, T2, T3, T4, T5, T6> StructuredCodec<R> object(TupleParser6<T1, T2, T3, T4, T5, T6, R> constructor,
			String field1, Function<R, T1> getter1, StructuredCodec<T1> codec1,
			String field2, Function<R, T2> getter2, StructuredCodec<T2> codec2,
			String field3, Function<R, T3> getter3, StructuredCodec<T3> codec3,
			String field4, Function<R, T4> getter4, StructuredCodec<T4> codec4,
			String field5, Function<R, T5> getter5, StructuredCodec<T5> codec5,
			String field6, Function<R, T6> getter6, StructuredCodec<T6> codec6) {
		return ofObjectMap(map(field1, codec1, field2, codec2, field3, codec3, field4, codec4, field5, codec5, field6, codec6))
				.transform(
						map -> constructor.create((T1) map.get(field1), (T2) map.get(field2), (T3) map.get(field3), (T4) map.get(field4), (T5) map.get(field5), (T6) map.get(field6)),
						item -> map(field1, getter1.apply(item), field2, getter2.apply(item), field3, getter3.apply(item), field4, getter4.apply(item), field5, getter5.apply(item), field6, getter6.apply(item)));
	}
}
