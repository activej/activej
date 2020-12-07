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

package io.activej.aggregation.util;

import io.activej.aggregation.Aggregate;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.measure.Measure;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.codec.StructuredCodec;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.impl.SerializerDefClass;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.activej.codec.StructuredCodecs.ofTupleArray;
import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.concat;
import static io.activej.common.collection.CollectionUtils.keysToMap;
import static io.activej.common.reflection.ReflectionUtils.extractFieldNameFromGetter;

@SuppressWarnings({"rawtypes", "unchecked"})
public class Utils {

	public static <K extends Comparable> Class<K> createKeyClass(Map<String, FieldType> keys, DefiningClassLoader classLoader) {
		List<String> keyList = new ArrayList<>(keys.keySet());
		return ClassBuilder.<K>create(classLoader, Comparable.class)
				.withClassKey(keyList)
				.withInitializer(cb ->
						keys.forEach((key, value) ->
								cb.withField(key, value.getInternalDataType())))
				.withMethod("compareTo", compareToImpl(keyList))
				.withMethod("equals", equalsImpl(keyList))
				.withMethod("hashCode", hashCodeImpl(keyList))
				.withMethod("toString", toStringImpl(keyList))
				.build();
	}

	public static <R> Comparator<R> createKeyComparator(Class<R> recordClass, List<String> keys, DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Comparator.class)
				.withClassKey(recordClass, keys)
				.withMethod("compare", compare(recordClass, keys))
				.buildClassAndCreateNewInstance();
	}

	public static <T, R> Function<T, R> createMapper(Class<T> recordClass, Class<R> resultClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withClassKey(recordClass, resultClass, keys, fields)
				.withMethod("apply",
						let(constructor(resultClass), result ->
								sequence(expressions -> {
									for (String fieldName : concat(keys, fields)) {
										expressions.add(set(
												property(result, fieldName),
												property(cast(arg(0), recordClass), fieldName)));
									}
									expressions.add(result);
								})))
				.buildClassAndCreateNewInstance();
	}

	public static <K extends Comparable, R> Function<R, K> createKeyFunction(Class<R> recordClass, Class<K> keyClass,
			List<String> keys,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withClassKey(recordClass, keyClass, keys)
				.withMethod("apply",
						let(constructor(keyClass), key ->
								sequence(expressions -> {
									for (String keyString : keys) {
										expressions.add(
												set(
														property(key, keyString),
														property(cast(arg(0), recordClass), keyString)));
									}
									expressions.add(key);
								})))
				.buildClassAndCreateNewInstance();
	}

	public static <T> Class<T> createRecordClass(AggregationStructure aggregation,
			Collection<String> keys, Collection<String> fields,
			DefiningClassLoader classLoader) {
		return createRecordClass(
				keysToMap(keys.stream(), aggregation.getKeyTypes()::get),
				keysToMap(fields.stream(), aggregation.getMeasureTypes()::get),
				classLoader);
	}

	public static <T> Class<T> createRecordClass(Map<String, FieldType> keys, Map<String, FieldType> fields,
			DefiningClassLoader classLoader) {
		List<String> keysList = new ArrayList<>(keys.keySet());
		List<String> fieldsList = new ArrayList<>(fields.keySet());
		return (Class<T>) ClassBuilder.create(classLoader, Object.class)
				.withClassKey(keysList, fieldsList)
				.withInitializer(cb ->
						keys.forEach((key, value) ->
								cb.withField(key, value.getInternalDataType())))
				.withInitializer(cb ->
						fields.forEach((key, value) ->
								cb.withField(key, value.getInternalDataType())))
				.withMethod("toString", toStringImpl(concat(keys.keySet(), fields.keySet())))
				.build();
	}

	public static <T> BinarySerializer<T> createBinarySerializer(AggregationStructure aggregation, Class<T> recordClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return createBinarySerializer(recordClass,
				keysToMap(keys.stream(), aggregation.getKeyTypes()::get),
				keysToMap(fields.stream(), aggregation.getMeasureTypes()::get),
				classLoader);
	}

	private static <T> BinarySerializer<T> createBinarySerializer(Class<T> recordClass,
			Map<String, FieldType> keys, Map<String, FieldType> fields,
			DefiningClassLoader classLoader) {
		SerializerDefClass serializer = SerializerDefClass.of(recordClass);
		addFields(recordClass, new ArrayList<>(keys.entrySet()), serializer);
		addFields(recordClass, new ArrayList<>(fields.entrySet()), serializer);

		ArrayList<String> keysList = new ArrayList<>(keys.keySet());
		ArrayList<String> fieldsList = new ArrayList<>(fields.keySet());
		return SerializerBuilder.create(classLoader)
				.withClassKey(recordClass, keysList, fieldsList)
				.build(serializer);
	}

	private static <T> void addFields(Class<T> recordClass, List<Entry<String, FieldType>> fields, SerializerDefClass serializer) {
		for (Entry<String, FieldType> entry : fields) {
			try {
				Field field = recordClass.getField(entry.getKey());
				serializer.addField(field, entry.getValue().getSerializer(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static <K extends Comparable, I, O, A> Reducer<K, I, O, A> aggregationReducer(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {

		return ClassBuilder.create(classLoader, Reducer.class)
				.withClassKey(inputClass, outputClass, keys, fields)
				.withMethod("onFirstItem",
						let(constructor(outputClass), accumulator ->
								sequence(expressions -> {
									for (String key : keys) {
										expressions.add(
												set(
														property(accumulator, key),
														property(cast(arg(2), inputClass), key)
												));
									}
									for (String field : fields) {
										expressions.add(
												aggregation.getMeasure(field)
														.initAccumulatorWithAccumulator(
																property(accumulator, field),
																property(cast(arg(2), inputClass), field)
														));
									}
									expressions.add(accumulator);
								})))
				.withMethod("onNextItem",
						sequence(expressions -> {
							for (String field : fields) {
								expressions.add(
										aggregation.getMeasure(field)
												.reduce(
														property(cast(arg(3), outputClass), field),
														property(cast(arg(2), inputClass), field)
												));
							}
							expressions.add(arg(3));
						}))
				.withMethod("onComplete", call(arg(0), "accept", arg(2)))
				.buildClassAndCreateNewInstance();
	}

	public static <I, O> Aggregate<O, Object> createPreaggregator(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
			Map<String, String> keyFields, Map<String, String> measureFields,
			DefiningClassLoader classLoader) {

		ArrayList<String> keysList = new ArrayList<>(keyFields.keySet());
		ArrayList<String> measuresList = new ArrayList<>(measureFields.keySet());
		return ClassBuilder.create(classLoader, Aggregate.class)
				.withClassKey(inputClass, outputClass, keysList, measuresList)
				.withMethod("createAccumulator",
						let(constructor(outputClass), accumulator ->
								sequence(expressions -> {
									for (Entry<String, String> entry : keyFields.entrySet()) {
										expressions.add(set(
												property(accumulator, entry.getKey()),
												property(cast(arg(0), inputClass), entry.getValue())));
									}
									for (Entry<String, String> entry : measureFields.entrySet()) {
										String measure = entry.getKey();
										String inputFields = entry.getValue();
										Measure aggregateFunction = aggregation.getMeasure(measure);

										expressions.add(aggregateFunction.initAccumulatorWithValue(
												property(accumulator, measure),
												inputFields == null ? null : property(cast(arg(0), inputClass), inputFields)));
									}
									expressions.add(accumulator);
								})))
				.withMethod("accumulate",
						sequence(expressions -> {
							for (Entry<String, String> entry : measureFields.entrySet()) {
								String measure = entry.getKey();
								String inputFields = entry.getValue();
								Measure aggregateFunction = aggregation.getMeasure(measure);

								expressions.add(aggregateFunction.accumulate(
										property(cast(arg(0), outputClass), measure),
										inputFields == null ? null : property(cast(arg(1), inputClass), inputFields)));
							}
						}))
				.buildClassAndCreateNewInstance();
	}

	private static final PartitionPredicate SINGLE_PARTITION = (t, u) -> true;

	public static <T> PartitionPredicate<T> singlePartition() {
		return SINGLE_PARTITION;
	}

	public static PartitionPredicate createPartitionPredicate(Class recordClass, List<String> partitioningKey,
			DefiningClassLoader classLoader) {
		if (partitioningKey.isEmpty())
			return singlePartition();

		return ClassBuilder.create(classLoader, PartitionPredicate.class)
				.withClassKey(recordClass, partitioningKey)
				.withMethod("isSamePartition", and(
						partitioningKey.stream()
								.map(keyComponent -> cmpEq(
										property(cast(arg(0), recordClass), keyComponent),
										property(cast(arg(1), recordClass), keyComponent)))))
				.buildClassAndCreateNewInstance();
	}

	public static <T> Map<String, String> scanKeyFields(Class<T> inputClass) {
		Map<String, String> keyFields = new LinkedHashMap<>();
		for (Field field : inputClass.getFields()) {
			Key annotation = field.getAnnotation(Key.class);
			if (annotation != null) {
				String value = annotation.value();
				keyFields.put("".equals(value) ? field.getName() : value, field.getName());
			}
		}
		for (Method method : inputClass.getMethods()) {
			Key annotation = method.getAnnotation(Key.class);
			if (annotation != null) {
				String value = annotation.value();
				keyFields.put("".equals(value) ? method.getName() : value, method.getName());
			}
		}
		checkArgument(!keyFields.isEmpty(), "Missing @Key annotations in %s", inputClass);
		return keyFields;
	}

	public static <T> Map<String, String> scanMeasureFields(Class<T> inputClass) {
		Map<String, String> measureFields = new LinkedHashMap<>();
		Measures annotation = inputClass.getAnnotation(Measures.class);
		if (annotation != null) {
			for (String measure : annotation.value()) {
				measureFields.put(measure, null);
			}
		}
		for (Field field : inputClass.getFields()) {
			annotation = field.getAnnotation(Measures.class);
			if (annotation != null) {
				for (String measure : annotation.value()) {
					measureFields.put(measure.equals("") ? field.getName() : measure, field.getName());
				}
			}
		}
		for (Method method : inputClass.getMethods()) {
			annotation = method.getAnnotation(Measures.class);
			if (annotation != null) {
				for (String measure : annotation.value()) {
					measureFields.put(measure.equals("") ? extractFieldNameFromGetter(method) : measure, method.getName());
				}
			}
		}
		checkArgument(!measureFields.isEmpty(), "Missing @Measure(s) annotations in %s", inputClass);
		return measureFields;
	}

	public static StructuredCodec<PrimaryKey> getPrimaryKeyCodec(AggregationStructure aggregation) {
		StructuredCodec<?>[] keyCodec = new StructuredCodec<?>[aggregation.getKeys().size()];
		for (int i = 0; i < aggregation.getKeys().size(); i++) {
			String key = aggregation.getKeys().get(i);
			FieldType keyType = aggregation.getKeyTypes().get(key);
			keyCodec[i] = keyType.getInternalCodec();
		}
		return ofTupleArray(keyCodec)
				.transform(PrimaryKey::ofArray, PrimaryKey::getArray);
	}

	public static <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> wrapException(Function<Throwable, Throwable> wrapFn) {
		return (v, e) -> e == null ?
				Promise.of(v) :
				Promise.ofException(wrapFn.apply(e));
	}
}
