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

package io.activej.cube;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.JsonWriter.WriteObject;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.aggregation.util.JsonCodec;
import io.activej.bytebuf.ByteBuf;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.attributes.AttributeResolver;
import io.activej.cube.attributes.AttributeResolver.AttributesFunction;
import io.activej.cube.attributes.AttributeResolver.KeyFunction;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static io.activej.codegen.expression.Expressions.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;

public final class Utils {

	public static <R> Class<R> createResultClass(Collection<String> attributes, Collection<String> measures,
			ReactiveCube cube, DefiningClassLoader classLoader) {
		return classLoader.ensureClass(
				ClassKey.of(Object.class, new HashSet<>(attributes), new HashSet<>(measures)),
				() -> {
					//noinspection unchecked
					ClassBuilder<R> builder = ClassBuilder.create((Class<R>) Object.class);
					for (String attribute : attributes) {
						builder.withField(attribute.replace('.', '$'), cube.getAttributeInternalType(attribute));
					}
					for (String measure : measures) {
						builder.withField(measure, cube.getMeasureInternalType(measure));
					}
					return builder;
				}
		);
	}

	static boolean startsWith(List<String> list, List<String> prefix) {
		if (prefix.size() >= list.size())
			return false;

		for (int i = 0; i < prefix.size(); ++i) {
			if (!list.get(i).equals(prefix.get(i)))
				return false;
		}

		return true;
	}

	@SuppressWarnings("unchecked")
	public static <R> Promise<Void> resolveAttributes(List<R> results, AttributeResolver attributeResolver,
			List<String> recordDimensions, List<String> recordAttributes,
			Map<String, Object> fullySpecifiedDimensions,
			Class<R> recordClass, DefiningClassLoader classLoader) {
		Object[] fullySpecifiedDimensionsArray = new Object[recordDimensions.size()];
		for (int i = 0; i < recordDimensions.size(); i++) {
			String dimension = recordDimensions.get(i);
			if (fullySpecifiedDimensions.containsKey(dimension)) {
				fullySpecifiedDimensionsArray[i] = fullySpecifiedDimensions.get(dimension);
			}
		}

		KeyFunction keyFunction = classLoader.ensureClassAndCreateInstance(
				ClassKey.of(KeyFunction.class, recordClass, recordDimensions, Arrays.asList(fullySpecifiedDimensionsArray)),
				() -> ClassBuilder.create(KeyFunction.class)
						.withMethod("extractKey",
								let(
										arrayNew(Object[].class, value(recordDimensions.size())),
										key -> sequence(seq -> {
											for (int i = 0; i < recordDimensions.size(); i++) {
												String dimension = recordDimensions.get(i);
												seq.add(arraySet(key, value(i),
														fullySpecifiedDimensions.containsKey(dimension) ?
																arrayGet(value(fullySpecifiedDimensionsArray), value(i)) :
																cast(property(cast(arg(0), recordClass), dimension), Object.class)));
											}
											return key;
										})))
		);

		AttributesFunction attributesFunction = classLoader.ensureClassAndCreateInstance(
				ClassKey.of(AttributesFunction.class, recordClass, new HashSet<>(recordAttributes)),
				() -> ClassBuilder.create(AttributesFunction.class)
						.withMethod("applyAttributes",
								sequence(seq -> {
									List<String> resolverAttributes = new ArrayList<>(attributeResolver.getAttributeTypes().keySet());
									for (String attribute : recordAttributes) {
										String attributeName = attribute.substring(attribute.indexOf('.') + 1);
										int resolverAttributeIndex = resolverAttributes.indexOf(attributeName);
										seq.add(set(
												property(cast(arg(0), recordClass), attribute.replace('.', '$')),
												arrayGet(arg(1), value(resolverAttributeIndex))));
									}
								}))
		);

		return attributeResolver.resolveAttributes((List<Object>) results, keyFunction, attributesFunction);
	}

	@SuppressWarnings("unchecked")
	public static <D, C> Set<C> chunksInDiffs(CubeDiffScheme<D> cubeDiffsExtractor,
			List<? extends D> diffs) {
		return diffs.stream()
				.flatMap(cubeDiffsExtractor::unwrapToStream)
				.flatMap(CubeDiff::addedChunks)
				.map(id -> (C) id)
				.collect(toSet());
	}

	public static final DslJson<?> CUBE_DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(CUBE_DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(CUBE_DSL_JSON::newReader);

	@SuppressWarnings("unchecked")
	public static JsonCodec<Object> getJsonCodec(Type type) {
		ReadObject<Object> readObject = (ReadObject<Object>) CUBE_DSL_JSON.tryFindReader(type);
		if (readObject == null) {
			throw new IllegalArgumentException("Cannot serialize " + type);
		}
		WriteObject<Object> writeObject = (WriteObject<Object>) CUBE_DSL_JSON.tryFindWriter(type);
		if (writeObject == null) {
			throw new IllegalArgumentException("Cannot deserialize " + type);
		}
		return JsonCodec.of(readObject, writeObject);
	}

	public static <T> String toJson(WriteObject<T> writeObject, @Nullable T object) {
		return toJsonWriter(writeObject, object).toString();
	}

	public static <T> ByteBuf toJsonBuf(WriteObject<T> writeObject, @Nullable T object) {
		return ByteBuf.wrapForReading(toJsonWriter(writeObject, object).toByteArray());
	}

	private static <T> JsonWriter toJsonWriter(WriteObject<T> writeObject, @Nullable T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		writeObject.write(jsonWriter, object);
		return jsonWriter;
	}

	public static <T> T fromJson(ReadObject<T> readObject, ByteBuf jsonBuf) throws MalformedDataException {
		return fromJson(readObject, jsonBuf.getArray());
	}

	public static <T> T fromJson(ReadObject<T> readObject, String json) throws MalformedDataException {
		return fromJson(readObject, json.getBytes(UTF_8));
	}

	private static <T> T fromJson(ReadObject<T> readObject, byte[] bytes) throws MalformedDataException {
		JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
		try {
			jsonReader.getNextToken();
			T deserialized = readObject.read(jsonReader);
			if (jsonReader.length() != jsonReader.getCurrentIndex()) {
				String unexpectedData = jsonReader.toString().substring(jsonReader.getCurrentIndex());
				throw new MalformedDataException("Unexpected JSON data: " + unexpectedData);
			}
			return deserialized;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
}
