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

package io.activej.cube.aggregation.util;

import io.activej.codegen.ClassGenerator;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.cube.AggregationStructure;
import io.activej.cube.aggregation.Aggregate;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ProtoAggregationChunk;
import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.aggregation.ot.ProtoAggregationDiff;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.aggregation.predicate.AggregationPredicate.ValueResolver;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.etl.LogDiff;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.def.AbstractSerializerDef;
import io.activej.serializer.def.PrimitiveSerializerDef;
import io.activej.serializer.def.SerializerDef;
import io.activej.serializer.def.impl.ClassSerializerDef;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.concat;
import static io.activej.common.Utils.toLinkedHashMap;
import static io.activej.common.reflection.ReflectionUtils.extractFieldNameFromGetter;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.transformHasPredicates;
import static io.activej.serializer.def.SerializerExpressions.readVarLong;
import static io.activej.serializer.def.SerializerExpressions.writeVarLong;

@SuppressWarnings({"rawtypes", "unchecked"})
public class Utils {

	public static <K extends Comparable> Class<K> createKeyClass(Map<String, FieldType> keys, DefiningClassLoader classLoader) {
		List<String> keyList = new ArrayList<>(keys.keySet());
		return classLoader.ensureClass(
			ClassKey.of(Object.class, keyList),
			() -> ClassGenerator.builder((Class<K>) Comparable.class)
				.initialize(b ->
					keys.forEach((key, value) ->
						b.withField(key, value.getInternalDataType())))
				.withMethod("compareTo", comparableImpl(keyList))
				.withMethod("equals", equalsImpl(keyList))
				.withMethod("hashCode", hashCodeImpl(keyList))
				.withMethod("toString", toStringImpl(keyList))
				.build());
	}

	public static <K extends Comparable> Class<K> createKeyClass(DefiningClassLoader classLoader, Map<String, Class<?>> keys) {
		List<String> keyList = new ArrayList<>(keys.keySet());
		return classLoader.ensureClass(
			ClassKey.of(Object.class, keyList),
			() -> ClassGenerator.builder((Class<K>) Comparable.class)
				.initialize(b -> keys.forEach(b::withField))
				.withMethod("compareTo", comparableImpl(keyList))
				.withMethod("equals", equalsImpl(keyList))
				.withMethod("hashCode", hashCodeImpl(keyList))
				.withMethod("toString", toStringImpl(keyList))
				.build());
	}

	public static <R> Comparator<R> createKeyComparator(
		Class<R> recordClass, List<String> keys, DefiningClassLoader classLoader
	) {
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(Comparator.class, recordClass, keys),
			() -> ClassGenerator.builder(Comparator.class)
				.withMethod("compare", comparatorImpl(recordClass, keys))
				.build());
	}

	public static <T, R> Function<T, R> createMapper(
		Class<T> recordClass, Class<R> resultClass, List<String> keys, List<String> fields,
		DefiningClassLoader classLoader
	) {
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(Function.class, recordClass, resultClass, keys, fields),
			() -> ClassGenerator.builder(Function.class)
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

	public static <K extends Comparable, R> Function<R, K> createKeyFunction(
		Class<R> recordClass, Class<K> keyClass, List<String> keys, DefiningClassLoader classLoader
	) {
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(Function.class, recordClass, keyClass, keys),
			() -> ClassGenerator.builder(Function.class)
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

	public static <T> Class<T> createRecordClass(
		AggregationStructure aggregation, Collection<String> keys, Collection<String> fields,
		DefiningClassLoader classLoader
	) {
		return createRecordClass(
			keys.stream()
				.collect(toLinkedHashMap(aggregation.getKeyTypes()::get)),
			fields.stream()
				.collect(toLinkedHashMap(aggregation.getMeasureTypes()::get)),
			classLoader);
	}

	public static <T> Class<T> createRecordClass(
		Map<String, FieldType> keys, Map<String, FieldType> fields, DefiningClassLoader classLoader
	) {
		List<String> keysList = new ArrayList<>(keys.keySet());
		List<String> fieldsList = new ArrayList<>(fields.keySet());
		return (Class<T>) classLoader.ensureClass(
			ClassKey.of(Object.class, keysList, fieldsList),
			() -> ClassGenerator.builder(Object.class)
				.initialize(b ->
					keys.forEach((key, value) ->
						b.withField(key, value.getInternalDataType())))
				.initialize(b ->
					fields.forEach((key, value) ->
						b.withField(key, value.getInternalDataType())))
				.withMethod("toString", toStringImpl(concat(keysList, fieldsList)))
				.build());
	}

	public static <T> BinarySerializer<T> createBinarySerializer(
		AggregationStructure aggregation, Class<T> recordClass, List<String> keys, List<String> fields,
		DefiningClassLoader classLoader
	) {
		return createBinarySerializer(recordClass,
			keys.stream()
				.collect(toLinkedHashMap(aggregation.getKeyTypes()::get)),
			fields.stream()
				.collect(toLinkedHashMap(aggregation.getMeasureTypes()::get)),
			classLoader);
	}

	private static <T> BinarySerializer<T> createBinarySerializer(
		Class<T> recordClass, Map<String, FieldType> keys, Map<String, FieldType> fields,
		DefiningClassLoader classLoader
	) {
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(BinarySerializer.class, recordClass, new ArrayList<>(keys.keySet()), new ArrayList<>(fields.keySet())),
			() -> SerializerFactory.defaultInstance()
				.toClassGenerator(
					ClassSerializerDef.builder(recordClass)
						.initialize(b -> addFields(b, recordClass, new ArrayList<>(keys.entrySet())))
						.initialize(b -> addFields(b, recordClass, new ArrayList<>(fields.entrySet())))
						.build()));
	}

	private static <T> void addFields(ClassSerializerDef.Builder classSerializerBuilder, Class<T> recordClass, List<Entry<String, FieldType>> fields) {
		for (Entry<String, FieldType> entry : fields) {
			try {
				classSerializerBuilder.withField(recordClass.getField(entry.getKey()), entry.getValue().getSerializer(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw new AssertionError(e);
			}
		}
	}

	public static <K extends Comparable, I, O, A> Reducer<K, I, O, A> aggregationReducer(
		AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass, List<String> keys,
		List<String> fields, Map<String, Measure> extraFields, DefiningClassLoader classLoader
	) {
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(Reducer.class, inputClass, outputClass, keys, fields, extraFields.keySet()),
			() -> ClassGenerator.builder(Reducer.class)
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

	public static <I, O> Aggregate<O, Object> createPreaggregator(
		AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass, Map<String, String> keyFields,
		Map<String, String> measureFields, DefiningClassLoader classLoader
	) {
		ArrayList<String> keysList = new ArrayList<>(keyFields.keySet());
		ArrayList<String> measuresList = new ArrayList<>(measureFields.keySet());
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(Aggregate.class, inputClass, outputClass, keysList, measuresList),
			() -> ClassGenerator.builder(Aggregate.class)
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

	public static PartitionPredicate createPartitionPredicate(
		Class recordClass, List<String> partitioningKey, DefiningClassLoader classLoader
	) {
		if (partitioningKey.isEmpty())
			return singlePartition();

		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(PartitionPredicate.class, recordClass, partitioningKey),
			() -> ClassGenerator.builder(PartitionPredicate.class)
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
					measureFields.put(measure.isEmpty() ? field.getName() : measure, field.getName());
				}
			}
		}
		for (Method method : inputClass.getMethods()) {
			annotation = method.getAnnotation(Measures.class);
			if (annotation != null) {
				for (String measure : annotation.value()) {
					measureFields.put(measure.isEmpty() ? extractFieldNameFromGetter(method) : measure, method.getName());
				}
			}
		}
		checkArgument(!measureFields.isEmpty(), "Missing @Measure(s) annotations in %s", inputClass);
		return measureFields;
	}

	public static Set<Long> collectChunkIds(Collection<AggregationChunk> chunks) {
		return chunks.stream().map(AggregationChunk::getChunkId).collect(Collectors.toSet());
	}

	public static <T> Predicate<T> createPredicateWithPrecondition(
		Class<T> chunkRecordClass, AggregationPredicate filter, AggregationPredicate precondition,
		@SuppressWarnings("rawtypes") Map<String, FieldType> fieldTypes, DefiningClassLoader classLoader,
		Function<String, @Nullable AggregationPredicate> validityPredicates
	) {
		AggregationPredicate simplifiedFilter = transformHasPredicates(filter, validityPredicates).simplify();
		AggregationPredicate simplifiedPrecondition = transformHasPredicates(precondition, validityPredicates).simplify();

		//noinspection unchecked
		return classLoader.ensureClassAndCreateInstance(
			ClassKey.of(Predicate.class, chunkRecordClass, simplifiedFilter, simplifiedPrecondition),
			() -> {
				Expression record = cast(arg(0), chunkRecordClass);
				ValueResolver valueResolver = createValueResolverOfFields(fieldTypes);
				return ClassGenerator.builder(Predicate.class)
					.withMethod("test", boolean.class, List.of(Object.class),
						ifElse(simplifiedFilter.createPredicate(record, valueResolver),
							ifElse(simplifiedPrecondition.createPredicate(record, valueResolver),
								value(true),
								throwException(IllegalStateException.class)
							),
							value(false)))
					.build();
			}
		);
	}

	@SuppressWarnings("unchecked")
	public static Object toInternalValue(Map<String, FieldType> fields, String key, Object value) {
		return fields.containsKey(key) ? fields.get(key).toInternalValue(value) : value;
	}

	@SuppressWarnings("rawtypes")
	private static Expression toStringValue(Map<String, FieldType> fields, String key, Expression value) {
		if (!fields.containsKey(key)) return value;

		FieldType fieldType = fields.get(key);
		Expression initialValue = fieldType.toValue(value);
		return fieldType.toStringValue(initialValue);
	}

	public static ValueResolver createValueResolverOfFields(Map<String, FieldType> fields) {
		return new ValueResolver() {
			@Override
			public Expression getProperty(Expression record, String key) {
				return property(record, key.replace('.', '$'));
			}

			@Override
			public Object transformArg(String key, Object value) {
				return toInternalValue(fields, key, value);
			}

			@Override
			public Expression toString(String key, Expression value) {
				return toStringValue(fields, key, value);
			}
		};
	}

	public static ValueResolver createValueResolverOfMeasures(Map<String, FieldType> fields, Map<String, Measure> measures) {
		return new ValueResolver() {
			@Override
			public Expression getProperty(Expression record, String key) {
				Variable property = property(record, key.replace('.', '$'));
				Measure measure = measures.get(key);
				return measure == null ? property : measure.valueOfAccumulator(property);
			}

			@Override
			public Object transformArg(String key, Object value) {
				return toInternalValue(fields, key, value);
			}

			@Override
			public Expression toString(String key, Expression value) {
				return toStringValue(fields, key, value);
			}
		};
	}

	public static boolean isArithmeticType(Type type) {
		return type == byte.class || type == Byte.class ||
			   type == short.class || type == Short.class ||
			   type == char.class || type == Character.class ||
			   type == int.class || type == Integer.class ||
			   type == long.class || type == Long.class ||
			   type == float.class || type == Float.class ||
			   type == double.class || type == Double.class;
	}

	public static boolean needsWideningToInt(Type type) {
		return type == byte.class || type == Byte.class ||
			   type == short.class || type == Short.class ||
			   type == char.class || type == Character.class;
	}

	public static <T> FieldType<T> valueWithTimestampFieldType(FieldType<T> fieldType) {
		SerializerDef serializerDef = valueWithTimestampSerializer(fieldType.getSerializer());
		JsonCodec<ValueWithTimestamp<T>> jsonCodec = JsonCodecs.ofObject(ValueWithTimestamp::new,
			"value", v -> v.value, fieldType.getJsonCodec(),
			"timestamp", v -> v.timestamp, JsonCodecs.ofLong());

		return new FieldType<>(ValueWithTimestamp.class, fieldType.getDataType(), serializerDef, fieldType.getJsonCodec(), jsonCodec) {
		};
	}

	private static SerializerDef valueWithTimestampSerializer(SerializerDef serializer) {
		if (serializer instanceof PrimitiveSerializerDef primitiveSerializerDef) {
			serializer = primitiveSerializerDef.ensureWrapped();
		}
		SerializerDef finalSerializer = serializer;
		return new AbstractSerializerDef() {
			@Override
			public Class<?> getEncodeType() {
				return ValueWithTimestamp.class;
			}

			@Override
			public void accept(Visitor visitor) {
				visitor.visit(finalSerializer);
			}

			@Override
			public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
				return finalSerializer.isInline(version, compatibilityLevel);
			}

			@Override
			public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
				Encoder encoder = finalSerializer.defineEncoder(staticEncoders, version, compatibilityLevel);
				return sequence(
					encoder.encode(buf, pos, cast(property(value, "value"), finalSerializer.getEncodeType())),
					writeVarLong(buf, pos, property(value, "timestamp"))
				);
			}

			@Override
			public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
				Decoder decoder = finalSerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
				return constructor(ValueWithTimestamp.class, decoder.decode(in), readVarLong(in));
			}
		};
	}

	public static Expression defaultValue(Class<?> cls) {
		if (!cls.isPrimitive()) return nullRef(cls);

		if (cls == boolean.class) return value(false, cls);
		if (cls == byte.class) return value((byte) 0, cls);
		if (cls == short.class) return value((short) 0, cls);
		if (cls == char.class) return value((char) 0, cls);
		if (cls == int.class) return value(0, cls);
		if (cls == long.class) return value(0L, cls);
		if (cls == float.class) return value(0f, cls);
		if (cls == double.class) return value(0.0, cls);
		throw new AssertionError();
	}

	public static final class ValueWithTimestamp<T> {
		public T value;
		public long timestamp;

		public ValueWithTimestamp(T value, long timestamp) {
			this.value = value;
			this.timestamp = timestamp;
		}
	}

	public static String escapeFilename(String filename) {
		return filename.replaceAll("\\W+", "");
	}

	public static AggregationDiff materializeProtoDiff(ProtoAggregationDiff protoAggregationDiff, Map<String, Long> idConformityMap) {
		Set<ProtoAggregationChunk> addedProtoChunks = protoAggregationDiff.addedChunks();
		Set<AggregationChunk> addedChunks = new HashSet<>(addedProtoChunks.size());
		for (ProtoAggregationChunk addedProtoChunk : addedProtoChunks) {
			long chunkId = idConformityMap.get(addedProtoChunk.protoChunkId());
			addedChunks.add(addedProtoChunk.toAggregationChunk(chunkId));
		}
		return AggregationDiff.of(addedChunks, protoAggregationDiff.removedChunks());
	}

	public static CubeDiff materializeProtoDiff(ProtoCubeDiff protoCubeDiff, Map<String, Long> idConformityMap) {
		Map<String, ProtoAggregationDiff> protoDiffs = protoCubeDiff.diffs();
		Map<String, AggregationDiff> diffs = new HashMap<>(protoDiffs.size());
		for (Entry<String, ProtoAggregationDiff> entry : protoDiffs.entrySet()) {
			diffs.put(entry.getKey(), materializeProtoDiff(entry.getValue(), idConformityMap));
		}

		return CubeDiff.of(diffs);
	}

	public static LogDiff<CubeDiff> materializeProtoDiff(LogDiff<ProtoCubeDiff> logDiff, Map<String, Long> idConformityMap) {
		return LogDiff.of(
			logDiff.getPositions(),
			logDiff.getDiffs().stream()
				.map(protoCubeDiff -> materializeProtoDiff(protoCubeDiff, idConformityMap))
				.toList()
		);
	}

	public static List<LogDiff<CubeDiff>> materializeProtoDiff(List<LogDiff<ProtoCubeDiff>> logDiffs, Map<String, Long> idConformityMap) {
		return logDiffs.stream()
			.map(logDiff -> materializeProtoDiff(logDiff, idConformityMap))
			.toList();
	}

}
