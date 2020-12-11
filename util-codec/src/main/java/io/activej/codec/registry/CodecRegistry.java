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

package io.activej.codec.registry;

import io.activej.codec.CodecSubtype;
import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredInput;
import io.activej.codec.StructuredOutput;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.reflection.RecursiveType;
import io.activej.common.tuple.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.common.Checks.checkNotNull;

/**
 * A registry which stores codecs by their type and allows dynamic dispatch of them.
 * <p>
 * Also it allows dynamic construction of codecs for generic types.
 */
public final class CodecRegistry implements CodecFactory {
	private final Map<Class<?>, BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>>> map = new HashMap<>();

	private CodecRegistry() {
	}

	/**
	 * Creates a new completely empty registry.
	 * You are advised to use {@link #createDefault()} factory method instead.
	 */
	public static CodecRegistry create() {
		return new CodecRegistry();
	}

	/**
	 * Creates a registry with a set of default codecs - primitives, some Java types, collections, ActiveJ tuples.
	 */
	public static CodecRegistry createDefault() {
		return create()
				.with(void.class, VOID_CODEC)
				.with(boolean.class, BOOLEAN_CODEC)
				.with(char.class, CHARACTER_CODEC)
				.with(byte.class, BYTE_CODEC)
				.with(int.class, INT_CODEC)
				.with(long.class, LONG_CODEC)
				.with(float.class, FLOAT_CODEC)
				.with(double.class, DOUBLE_CODEC)

				.with(Void.class, VOID_CODEC)
				.with(Boolean.class, BOOLEAN_CODEC)
				.with(Character.class, CHARACTER_CODEC)
				.with(Byte.class, BYTE_CODEC)
				.with(Integer.class, INT_CODEC)
				.with(Long.class, LONG_CODEC)
				.with(Float.class, FLOAT_CODEC)
				.with(Double.class, DOUBLE_CODEC)

				.with(String.class, STRING_CODEC)

				.with(byte[].class, BYTES_CODEC)

				.withGeneric(Optional.class, (registry, subCodecs) ->
						ofOptional(subCodecs[0]))

				.withGeneric(Set.class, (registry, subCodecs) ->
						ofSet(subCodecs[0]))
				.withGeneric(List.class, (registry, subCodecs) ->
						ofList(subCodecs[0]))
				.withGeneric(Map.class, (registry, subCodecs) ->
						ofMap(subCodecs[0], subCodecs[1]))

				.withGeneric(Tuple1.class, (registry, subCodecs) ->
						tuple(Tuple1::new,
								Tuple1::getValue1, subCodecs[0]))
				.withGeneric(Tuple2.class, (registry, subCodecs) ->
						tuple(Tuple2::new,
								Tuple2::getValue1, subCodecs[0],
								Tuple2::getValue2, subCodecs[1]))
				.withGeneric(Tuple3.class, (registry, subCodecs) ->
						tuple(Tuple3::new,
								Tuple3::getValue1, subCodecs[0],
								Tuple3::getValue2, subCodecs[1],
								Tuple3::getValue3, subCodecs[2]))
				.withGeneric(Tuple4.class, (registry, subCodecs) ->
						tuple(Tuple4::new,
								Tuple4::getValue1, subCodecs[0],
								Tuple4::getValue2, subCodecs[1],
								Tuple4::getValue3, subCodecs[2],
								Tuple4::getValue4, subCodecs[3]))
				.withGeneric(Tuple5.class, (registry, subCodecs) ->
						tuple(Tuple5::new,
								Tuple5::getValue1, subCodecs[0],
								Tuple5::getValue2, subCodecs[1],
								Tuple5::getValue3, subCodecs[2],
								Tuple5::getValue4, subCodecs[3],
								Tuple5::getValue5, subCodecs[4]))
				.withGeneric(Tuple6.class, (registry, subCodecs) ->
						tuple(Tuple6::new,
								Tuple6::getValue1, subCodecs[0],
								Tuple6::getValue2, subCodecs[1],
								Tuple6::getValue3, subCodecs[2],
								Tuple6::getValue4, subCodecs[3],
								Tuple6::getValue5, subCodecs[4],
								Tuple6::getValue6, subCodecs[5]))
				;
	}

	public <T> CodecRegistry with(Class<T> type, StructuredCodec<T> codec) {
		return withGeneric(type, (self, $) -> codec);
	}

	public <T> CodecRegistry with(Class<T> type, Function<CodecFactory, StructuredCodec<T>> codec) {
		return withGeneric(type, (self, $) -> codec.apply(self));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> CodecRegistry withGeneric(Class<T> type, BiFunction<CodecFactory, StructuredCodec<Object>[], StructuredCodec<? extends T>> fn) {
		map.put(type, (BiFunction) fn);
		return this;
	}

	public <T> CodecRegistry withSubtypesOf(Class<T> type) {
		return withSubtypesOf(type, $ -> null);
	}

	@SuppressWarnings("unchecked")
	public <T> CodecRegistry withSubtypesOf(Class<T> type, Function<Class<? extends T>, @Nullable String> subtypeNameFactory) {
		return withGeneric(type, (self, $) ->
				new LazyCodec<>(() -> {
					CodecSubtype<T> subtypeCodec = CodecSubtype.create();
					for (Class<?> subtype : map.keySet()) {
						if (type == subtype || !type.isAssignableFrom(subtype)) {
							continue;
						}

						String name = subtypeNameFactory.apply((Class<? extends T>) subtype);
						if (name != null) {
							subtypeCodec.with(subtype, name, self.get((Type) subtype));
						} else {
							subtypeCodec.with(subtype, self.get((Type) subtype));
						}
					}
					return subtypeCodec;
				}));
	}

	@Override
	public <T> StructuredCodec<T> get(Type type) {
		return doGet(RecursiveType.of(type));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private <T> StructuredCodec<T> doGet(RecursiveType type) {
		Class clazz = type.getRawType();
		if (Enum.class.isAssignableFrom(clazz)) {
			return ofEnum(clazz);
		}

		BiFunction<CodecFactory, StructuredCodec<?>[], StructuredCodec<?>> fn = checkNotNull(map.get(clazz));

		StructuredCodec<Object>[] subCodecs = new StructuredCodec[type.getTypeParams().length];

		RecursiveType[] typeParams = type.getTypeParams();
		for (int i = 0; i < typeParams.length; i++) {
			subCodecs[i] = doGet(typeParams[i]);
		}

		return (StructuredCodec<T>) fn.apply(this, subCodecs);
	}

	private static final class LazyCodec<T> implements StructuredCodec<T> {
		private StructuredCodec<T> ref;
		private final Supplier<StructuredCodec<T>> supplier;

		LazyCodec(Supplier<StructuredCodec<T>> supplier) {
			this.supplier = supplier;
		}

		@Override
		public T decode(StructuredInput in) throws MalformedDataException {
			if (ref == null) {
				ref = supplier.get();
			}
			return ref.decode(in);
		}

		@Override
		public void encode(StructuredOutput out, T item) {
			if (ref == null) {
				ref = supplier.get();
			}
			ref.encode(out, item);
		}
	}
}
