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

package io.activej.serializer.stream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.activej.serializer.stream.StreamCodecs.*;

/**
 * A registry which stores codecs by their type and allows dynamic dispatch of them.
 * <p>
 * Also it allows dynamic construction of codecs for generic types.
 */
public final class StreamCodecRegistry implements StreamCodecFactory {
	private final Map<Class<?>, BiFunction<StreamCodecFactory, StreamCodec<?>[], StreamCodec<?>>> map = new LinkedHashMap<>();

	private StreamCodecRegistry() {
	}

	/**
	 * Creates a new completely empty registry.
	 * You are advised to use {@link #createDefault()} factory method instead.
	 */
	public static StreamCodecRegistry create() {
		return new StreamCodecRegistry();
	}

	/**
	 * Creates a registry with a set of default codecs - primitives, some Java types and collections.
	 */
	public static StreamCodecRegistry createDefault() {
		return create()
				.with(void.class, ofVoid())
				.with(boolean.class, ofBoolean())
				.with(char.class, ofChar())
				.with(byte.class, ofByte())
				.with(int.class, ofInt())
				.with(long.class, ofLong())
				.with(float.class, ofFloat())
				.with(double.class, ofDouble())

				.with(Void.class, ofVoid())
				.with(Boolean.class, ofBoolean())
				.with(Character.class, ofChar())
				.with(Byte.class, ofByte())
				.with(Integer.class, ofInt())
				.with(Long.class, ofLong())
				.with(Float.class, ofFloat())
				.with(Double.class, ofDouble())

				.with(boolean[].class, ofBooleanArray())
				.with(char[].class, ofCharArray())
				.with(byte[].class, ofByteArray())
				.with(short[].class, ofShortArray())
				.with(int[].class, ofIntArray())
				.with(long[].class, ofLongArray())
				.with(float[].class, ofFloatArray())
				.with(double[].class, ofDoubleArray())

				.with(String.class, ofString())

				.withGeneric(Optional.class, (registry, subCodecs) ->
						ofOptional(subCodecs[0]))

				.withGeneric(Set.class, (registry, subCodecs) ->
						ofSet(subCodecs[0]))
				.withGeneric(List.class, (registry, subCodecs) ->
						ofList(subCodecs[0]))
				.withGeneric(Map.class, (registry, subCodecs) ->
						ofMap(subCodecs[0], subCodecs[1]))
				;
	}

	public <T> StreamCodecRegistry with(Class<T> type, StreamCodec<T> codec) {
		return withGeneric(type, (self, $) -> codec);
	}

	public <T> StreamCodecRegistry with(Class<T> type, Function<StreamCodecFactory, StreamCodec<T>> codec) {
		return withGeneric(type, (self, $) -> codec.apply(self));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> StreamCodecRegistry withGeneric(Class<T> type, BiFunction<StreamCodecFactory, StreamCodec<Object>[], StreamCodec<? extends T>> fn) {
		map.put(type, (BiFunction) fn);
		return this;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public <T> StreamCodec<T> get(Type type) {
		Class<?> rawType;
		StreamCodec<Object>[] subCodecs = null;

		if (type instanceof Class) {
			rawType = (Class<?>) type;
			if (Enum.class.isAssignableFrom(rawType)) {
				return ofEnum((Class) rawType);
			}

			if (rawType.isArray() && !map.containsKey(rawType)) {
				Class<?> componentClazz = rawType.getComponentType();

				StreamCodec<Object> componentCodec = this.get((Type) componentClazz);
				StreamCodec<Object[]> codec = ofArray(componentCodec);
				return (StreamCodec<T>) codec;
			}

		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			rawType = (Class) parameterizedType.getRawType();

			subCodecs = new StreamCodec[parameterizedType.getActualTypeArguments().length];

			for (int i = 0; i < subCodecs.length; i++) {
				subCodecs[i] = get(parameterizedType.getActualTypeArguments()[i]);
			}

		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}

		BiFunction<StreamCodecFactory, StreamCodec<?>[], StreamCodec<?>> fn = map.get(rawType);
		if (fn == null)
			throw new IllegalArgumentException("Codec is not registered for " + type);
		return (StreamCodec<T>) fn.apply(this, subCodecs);

	}

}
