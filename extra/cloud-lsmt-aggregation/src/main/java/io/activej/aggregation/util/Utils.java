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
import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.measure.Measure;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.impl.SerializerDef_Class;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.concat;
import static io.activej.common.Utils.keysToMap;
import static io.activej.common.reflection.ReflectionUtils.extractFieldNameFromGetter;

@SuppressWarnings({"rawtypes", "unchecked"})
public class Utils {

	public static <K extends Comparable> Class<K> createKeyClass(Map<String, FieldType> keys, DefiningClassLoader classLoader) {
		List<String> keyList = new ArrayList<>(keys.keySet());
		return classLoader.ensureClass(
				ClassKey.of(Object.class, keyList),
				() -> ClassBuilder.builder((Class<K>) Comparable.class)
						.initialize(cb ->
								keys.forEach((key, value) ->
										cb.withField(key, value.getInternalDataType())))
						.withMethod("compareTo", comparableImpl(keyList))
						.withMethod("equals", equalsImpl(keyList))
						.withMethod("hashCode", hashCodeImpl(keyList))
						.withMethod("toString", toStringImpl(keyList))
						.build());
	}

	public static <R> Comparator<R> createKeyComparator(Class<R> recordClass, List<String> keys,
			DefiningClassLoader classLoader) {
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(Comparator.class, recordClass, keys),
				() -> ClassBuilder.builder(Comparator.class)
						.withMethod("compare", comparatorImpl(recordClass, keys))
						.build());
	}

	public static <T, R> Function<T, R> createMapper(Class<T> recordClass, Class<R> resultClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(Function.class, recordClass, resultClass, keys, fields),
				() -> ClassBuilder.builder(Function.class)
						.withMethod("apply",
								let(constructor(resultClass), result ->
										sequence(seq -> {
											for (String fieldName : concat(keys, fields)) {
												seq.add(set(
														property(result, fieldName),
														property(cast(arg(0), recordClass), fieldName)));
											}
											return result;
										})))
						.build());
	}

	public static <K extends Comparable, R> Function<R, K> createKeyFunction(Class<R> recordClass, Class<K> keyClass,
			List<String> keys,
			DefiningClassLoader classLoader) {
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(Function.class, recordClass, keyClass, keys),
				() -> ClassBuilder.builder(Function.class)
						.withMethod("apply",
								let(constructor(keyClass), key ->
										sequence(seq -> {
											for (String keyString : keys) {
												seq.add(
														set(
																property(key, keyString),
																property(cast(arg(0), recordClass), keyString)));
											}
											return key;
										})))
						.build());
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
		return (Class<T>) classLoader.ensureClass(
				ClassKey.of(Object.class, keysList, fieldsList),
				() -> ClassBuilder.builder(Object.class)
						.initialize(cb ->
								keys.forEach((key, value) ->
										cb.withField(key, value.getInternalDataType())))
						.initialize(cb ->
								fields.forEach((key, value) ->
										cb.withField(key, value.getInternalDataType())))
						.withMethod("toString", toStringImpl(concat(keysList, fieldsList)))
						.build());
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
		ArrayList<String> keysList = new ArrayList<>(keys.keySet());
		ArrayList<String> fieldsList = new ArrayList<>(fields.keySet());
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(BinarySerializer.class, recordClass, keysList, fieldsList),
				() -> {
					SerializerDef_Class serializer = SerializerDef_Class.create(recordClass);
					addFields(serializer, recordClass, new ArrayList<>(keys.entrySet()));
					addFields(serializer, recordClass, new ArrayList<>(fields.entrySet()));

					return SerializerFactory.defaultInstance()
							.toClassBuilder(serializer);
				});
	}

	private static <T> void addFields(SerializerDef_Class serializer, Class<T> recordClass, List<Entry<String, FieldType>> fields) {
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
			List<String> keys, List<String> fields, Map<String, Measure> extraFields,
			DefiningClassLoader classLoader) {

		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(Reducer.class, inputClass, outputClass, keys, fields, extraFields.keySet()),
				() -> ClassBuilder.builder(Reducer.class)
						.withMethod("onFirstItem",
								let(constructor(outputClass), accumulator ->
										sequence(seq -> {
											for (String key : keys) {
												seq.add(
														set(
																property(accumulator, key),
																property(cast(arg(2), inputClass), key)
														));
											}
											for (String field : fields) {
												seq.add(
														aggregation.getMeasure(field)
																.initAccumulatorWithAccumulator(
																		property(accumulator, field),
																		property(cast(arg(2), inputClass), field)
																));
											}
											for (Entry<String, Measure> entry : extraFields.entrySet()) {
												seq.add(entry.getValue()
														.zeroAccumulator(property(accumulator, entry.getKey())));
											}
											return accumulator;
										})))
						.withMethod("onNextItem",
								sequence(seq -> {
									for (String field : fields) {
										seq.add(
												aggregation.getMeasure(field)
														.reduce(
																property(cast(arg(3), outputClass), field),
																property(cast(arg(2), inputClass), field)
														));
									}
									return arg(3);
								}))
						.withMethod("onComplete", call(arg(0), "accept", arg(2)))
						.build());
	}

	public static <I, O> Aggregate<O, Object> createPreaggregator(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
			Map<String, String> keyFields, Map<String, String> measureFields,
			DefiningClassLoader classLoader) {

		ArrayList<String> keysList = new ArrayList<>(keyFields.keySet());
		ArrayList<String> measuresList = new ArrayList<>(measureFields.keySet());
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(Aggregate.class, inputClass, outputClass, keysList, measuresList),
				() -> ClassBuilder.builder(Aggregate.class)
						.withMethod("createAccumulator",
								let(constructor(outputClass), accumulator ->
										sequence(seq -> {
											for (Entry<String, String> entry : keyFields.entrySet()) {
												seq.add(set(
														property(accumulator, entry.getKey()),
														property(cast(arg(0), inputClass), entry.getValue())));
											}
											for (Entry<String, String> entry : measureFields.entrySet()) {
												String measure = entry.getKey();
												String inputFields = entry.getValue();
												Measure aggregateFunction = aggregation.getMeasure(measure);

												seq.add(aggregateFunction.initAccumulatorWithValue(
														property(accumulator, measure),
														inputFields == null ? null : property(cast(arg(0), inputClass), inputFields)));
											}
											return accumulator;
										})))
						.withMethod("accumulate",
								sequence(seq -> {
									for (Entry<String, String> entry : measureFields.entrySet()) {
										String measure = entry.getKey();
										String inputFields = entry.getValue();
										Measure aggregateFunction = aggregation.getMeasure(measure);

										seq.add(aggregateFunction.accumulate(
												property(cast(arg(0), outputClass), measure),
												inputFields == null ? null : property(cast(arg(1), inputClass), inputFields)));
									}
								}))
						.build());
	}

	private static final PartitionPredicate SINGLE_PARTITION = (t, u) -> true;

	public static <T> PartitionPredicate<T> singlePartition() {
		return SINGLE_PARTITION;
	}

	public static PartitionPredicate createPartitionPredicate(Class recordClass, List<String> partitioningKey,
			DefiningClassLoader classLoader) {
		if (partitioningKey.isEmpty())
			return singlePartition();

		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(PartitionPredicate.class, recordClass, partitioningKey),
				() -> ClassBuilder.builder(PartitionPredicate.class)
						.withMethod("isSamePartition", and(
								partitioningKey.stream()
										.map(keyComponent -> isEq(
												property(cast(arg(0), recordClass), keyComponent),
												property(cast(arg(1), recordClass), keyComponent)))))
						.build());
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
				measureFields.put(measure, "");
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

	public static <C> Set<C> collectChunkIds(Collection<AggregationChunk> chunks) {
		return (Set<C>) chunks.stream().map(AggregationChunk::getChunkId).collect(Collectors.toSet());
	}
}
