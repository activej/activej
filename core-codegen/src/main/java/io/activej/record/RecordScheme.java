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

package io.activej.record;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.common.builder.AbstractBuilder;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

import static io.activej.codegen.expression.Expression.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("unused")
public final class RecordScheme {
	private final RecordFactory factory;

	private final RecordGetter<?>[] recordGetters;
	private final RecordSetter<?>[] recordSetters;

	private final HashMap<String, RecordGetter<?>> recordGettersMap;
	private final HashMap<String, RecordSetter<?>> recordSettersMap;

	private final String[] fields;
	private final Type[] types;

	private final List<String> fieldsList;
	private final List<Type> typesList;

	private final LinkedHashMap<String, Type> fieldToType;
	private final LinkedHashMap<String, Integer> fieldToIndex;

	private final @Nullable Comparator<Record> comparator;
	private final @Nullable List<String> comparedFields;

	private final Class<? extends Record> recordClass;
	private final Map<String, String> recordClassFields;

	private final DefiningClassLoader classLoader;

	private RecordScheme(Builder builder) {
		Collection<String> hashCodeEqualsFields;
		if (builder.hashCodeEqualsFields != null) {
			Set<String> missing = builder.getMissingFields(builder.hashCodeEqualsFields);
			if (!missing.isEmpty()) {
				throw new IllegalStateException("Missing some fields to generate 'hashCode' and 'equals' methods: " + missing);
			}
			hashCodeEqualsFields = builder.hashCodeEqualsFields;
		} else {
			hashCodeEqualsFields = builder.fieldToType.keySet();
		}
		List<String> hashCodeEqualsClassFields = hashCodeEqualsFields.stream()
				.map(builder::getClassField)
				.collect(Collectors.toList());
		this.recordClass = builder.classLoader.ensureClass(
				ClassKey.of(Record.class, asList(builder.fields), asList(builder.types), hashCodeEqualsClassFields),
				() -> ClassBuilder.create(Record.class)
						.withConstructor(List.of(RecordScheme.class),
								superConstructor(arg(0)))
						.withMethod("hashCode", hashCodeImpl(hashCodeEqualsClassFields))
						.withMethod("equals", equalsImpl(hashCodeEqualsClassFields))
						.initialize(b -> {
							for (Map.Entry<String, Type> entry : builder.fieldToType.entrySet()) {
								Type type = entry.getValue();
								//noinspection rawtypes
								b.withField(builder.getClassField(entry.getKey()), type instanceof Class ? ((Class) type) : Object.class);
							}
						}));
		this.recordGettersMap = new HashMap<>();
		this.recordSettersMap = new HashMap<>();
		this.recordGetters = new RecordGetter[builder.size()];
		this.recordSetters = new RecordSetter[builder.size()];
		for (Map.Entry<String, Type> entry : builder.fieldToType.entrySet()) {
			String field = entry.getKey();
			Type fieldType = entry.getValue();
			Variable property = builder.property(cast(arg(0), recordClass), field);
			RecordGetter<?> recordGetter = builder.classLoader.ensureClassAndCreateInstance(
					ClassKey.of(RecordGetter.class, recordClass, field),
					() -> ClassBuilder.create(RecordGetter.class)
							.withMethod("get", property)
							.initialize(cb -> {
								if (isImplicitType(fieldType)) {
									cb.withMethod("getInt", property);
									cb.withMethod("getLong", property);
									cb.withMethod("getFloat", property);
									cb.withMethod("getDouble", property);
								}
							})
							.withMethod("getScheme", value(this))
							.withMethod("getField", value(field))
							.withMethod("getType", value(fieldType, Type.class))
			);
			recordGetters[recordGettersMap.size()] = recordGetter;
			recordGettersMap.put(field, recordGetter);

			Expression set = Expression.set(property, arg(1));
			RecordSetter<?> recordSetter = builder.classLoader.ensureClassAndCreateInstance(
					ClassKey.of(RecordSetter.class, recordClass, field),
					() -> ClassBuilder.create(RecordSetter.class)
							.withMethod("set", set)
							.initialize(cb -> {
								if (isImplicitType(fieldType)) {
									cb.withMethod("setInt", set);
									cb.withMethod("setLong", set);
									cb.withMethod("setFloat", set);
									cb.withMethod("setDouble", set);
								}
							})
							.withMethod("getScheme", value(this))
							.withMethod("getField", value(field))
							.withMethod("getType", value(fieldType, Type.class)));
			recordSetters[recordSettersMap.size()] = recordSetter;
			recordSettersMap.put(field, recordSetter);
		}
		Comparator<Record> comparator = null;
		if (builder.comparedFields != null) {
			Set<String> missing = builder.getMissingFields(builder.comparedFields);
			if (!missing.isEmpty()) {
				throw new IllegalStateException("Missing some fields to be compared: " + missing);
			}

			List<String> comparedClassFields = builder.comparedFields.stream()
					.map(builder::getClassField)
					.toList();

			//noinspection unchecked
			comparator = builder.classLoader.ensureClassAndCreateInstance(
					ClassKey.of(Comparator.class, recordClass, builder.comparedFields),
					() -> ClassBuilder.create(Comparator.class)
							.withMethod("compare", comparatorImpl(recordClass, comparedClassFields)));
		}

		this.factory = builder.classLoader.ensureClassAndCreateInstance(
				ClassKey.of(RecordFactory.class, recordClass),
				() -> ClassBuilder.create(RecordFactory.class)
						.withStaticFinalField("SCHEME", RecordScheme.class, value(this))
						.withMethod("create", Record.class, List.of(),
								constructor(recordClass, staticField("SCHEME"))));

		this.fieldToType = builder.fieldToType;
		this.fieldToIndex = builder.fieldToIndex;
		this.fields = builder.fields;
		this.types = builder.types;
		this.fieldsList = Arrays.asList(this.fields);
		this.typesList = Arrays.asList(this.types);
		this.recordClassFields = builder.recordClassFields;
		this.comparator = comparator;
		this.comparedFields = builder.comparedFields;
		this.classLoader = builder.classLoader;
	}

	public static Builder builder() {
		return builder(DefiningClassLoader.create());
	}

	public static Builder builder(DefiningClassLoader classLoader) {
		return new Builder(classLoader);
	}

	public static final class Builder extends AbstractBuilder<Builder, RecordScheme> {
		private final DefiningClassLoader classLoader;

		private final LinkedHashMap<String, Type> fieldToType = new LinkedHashMap<>();
		private final LinkedHashMap<String, Integer> fieldToIndex = new LinkedHashMap<>();
		private String[] fields = {};
		private Type[] types = {};
		private final Map<String, String> recordClassFields = new HashMap<>();

		private @Nullable List<String> hashCodeEqualsFields;
		private @Nullable List<String> comparedFields;

		private Builder(DefiningClassLoader classLoader) {
			this.classLoader = classLoader;
		}

		@SuppressWarnings("UnusedReturnValue")
		public Builder withField(String field, Type type) {
			checkNotBuilt(this);
			if (fieldToType.containsKey(field)) throw new IllegalArgumentException("Duplicate field");
			fieldToType.put(field, type);
			fieldToIndex.put(field, fieldToIndex.size());
			fields = Arrays.copyOf(fields, fields.length + 1);
			fields[fields.length - 1] = field;
			types = Arrays.copyOf(types, types.length + 1);
			types[types.length - 1] = type;

			char[] chars = (Character.isJavaIdentifierStart(field.charAt(0)) ? field : "_" + field).toCharArray();
			for (int i = 1; i < chars.length; i++) {
				if (!Character.isJavaIdentifierPart(chars[i])) {
					chars[i] = '_';
				}
			}
			String sanitized = new String(chars);

			for (int i = 1; ; i++) {
				String recordClassField = i == 1 ? sanitized : sanitized + i;
				if (!recordClassFields.containsKey(recordClassField)) {
					recordClassFields.put(field, recordClassField);
					break;
				}
			}
			return this;
		}

		public Builder withHashCodeEqualsFields(List<String> hashCodeEqualsFields) {
			checkNotBuilt(this);
			checkUnique(hashCodeEqualsFields);
			this.hashCodeEqualsFields = hashCodeEqualsFields;
			return this;
		}

		public Builder withHashCodeEqualsFields(String... hashCodeEqualsFields) {
			checkNotBuilt(this);
			return withHashCodeEqualsFields(List.of(hashCodeEqualsFields));
		}

		public Builder withComparator(List<String> comparedFields) {
			checkNotBuilt(this);
			checkUnique(comparedFields);
			this.comparedFields = comparedFields;
			return this;
		}

		public Builder withComparator(String... comparedFields) {
			checkNotBuilt(this);
			return withComparator(List.of(comparedFields));
		}

		@Override
		protected RecordScheme doBuild() {
			return new RecordScheme(this);
		}

		private String getClassField(String field) {
			return recordClassFields.get(field);
		}

		private Set<String> getMissingFields(List<String> fields) {
			return fields.stream()
					.filter(field -> !fieldToType.containsKey(field))
					.collect(toSet());
		}

		private Variable property(Expression record, String field) {
			return Expression.property(record, getClassField(field));
		}

		private int size() {
			return fields.length;
		}
	}

	public Record record() {
		return factory.create();
	}

	public Record recordOfArray(Object... values) {
		Record record = record();
		record.setArray(values);
		return record;
	}

	public Record recordOfMap(Map<String, Object> values) {
		Record record = record();
		record.setMap(values);
		return record;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public Class<? extends Record> getRecordClass() {
		return recordClass;
	}

	public Comparator<Record> getRecordComparator() {
		return comparator;
	}

	public String getRecordClassField(String field) {
		return recordClassFields.get(field);
	}

	public Variable property(Expression record, String field) {
		return Expression.property(record, getRecordClassField(field));
	}

	public List<String> getFields() {
		return fieldsList;
	}

	public List<Type> getTypes() {
		return typesList;
	}

	public String getField(int index) {
		return fields[index];
	}

	public Type getFieldType(String field) {
		return fieldToType.get(field);
	}

	public Type getFieldType(int field) {
		return types[field];
	}

	public int getFieldIndex(String field) {
		return fieldToIndex.get(field);
	}

	@SuppressWarnings("NullableProblems")
	public List<String> getComparedFields() {
		return comparedFields;
	}

	public int size() {
		return fields.length;
	}

	private static boolean isImplicitType(Type fieldType) {
		return fieldType == byte.class || fieldType == short.class || fieldType == int.class || fieldType == long.class || fieldType == float.class || fieldType == double.class ||
				fieldType == Byte.class || fieldType == Short.class || fieldType == Integer.class || fieldType == Long.class || fieldType == Float.class || fieldType == Double.class;
	}

	private static void checkUnique(List<String> fields) {
		if (new HashSet<>(fields).size() != fields.size()) {
			throw new IllegalArgumentException("Fields should be unique");
		}
	}

	public <T> RecordGetter<T> getter(String field) {
		//noinspection unchecked
		return (RecordGetter<T>) recordGettersMap.get(field);
	}

	public <T> RecordGetter<T> getter(int field) {
		//noinspection unchecked
		return (RecordGetter<T>) recordGetters[field];
	}

	public <T> T get(Record record, String field) {
		//noinspection unchecked
		return (T) getter(field).get(record);
	}

	public <T> T get(Record record, int field) {
		//noinspection unchecked
		return (T) getter(field).get(record);
	}

	public int getInt(Record record, String field) {
		return getter(field).getInt(record);
	}

	public int getInt(Record record, int field) {
		return getter(field).getInt(record);
	}

	public long getLong(Record record, String field) {
		return getter(field).getLong(record);
	}

	public long getLong(Record record, int field) {
		return getter(field).getLong(record);
	}

	public float getFloat(Record record, String field) {
		return getter(field).getFloat(record);
	}

	public float getFloat(Record record, int field) {
		return getter(field).getFloat(record);
	}

	public double getDouble(Record record, String field) {
		return getter(field).getDouble(record);
	}

	public double getDouble(Record record, int field) {
		return getter(field).getDouble(record);
	}

	public <T> RecordSetter<T> setter(String field) {
		//noinspection unchecked
		return (RecordSetter<T>) recordSettersMap.get(field);
	}

	public <T> RecordSetter<T> setter(int field) {
		//noinspection unchecked
		return (RecordSetter<T>) recordSetters[field];
	}

	public <T> void set(Record record, String field, T value) {
		setter(field).set(record, value);
	}

	public <T> void set(Record record, int field, T value) {
		setter(field).set(record, value);
	}

	public void setInt(Record record, String field, int value) {
		setter(field).setInt(record, value);
	}

	public void setInt(Record record, int field, int value) {
		setter(field).setInt(record, value);
	}

	public void setLong(Record record, String field, long value) {
		setter(field).setLong(record, value);
	}

	public void setLong(Record record, int field, long value) {
		setter(field).setLong(record, value);
	}

	public void setFloat(Record record, String field, float value) {
		setter(field).setFloat(record, value);
	}

	public void setFloat(Record record, int field, float value) {
		setter(field).setFloat(record, value);
	}

	public void setDouble(Record record, String field, double value) {
		setter(field).setDouble(record, value);
	}

	public void setDouble(Record record, int field, double value) {
		setter(field).setDouble(record, value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RecordScheme scheme = (RecordScheme) o;
		return Arrays.equals(fields, scheme.fields) &&
				Arrays.equals(types, scheme.types) &&
				Objects.equals(comparedFields, scheme.comparedFields);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fields);
		result = 31 * result + Arrays.hashCode(types);
		result = 31 * result + Objects.hashCode(comparedFields);
		return result;
	}

	@Override
	public String toString() {
		return fieldToType.entrySet().stream()
				.map(entry -> entry.getKey() + "=" +
						(entry.getValue() instanceof Class ? ((Class<?>) entry.getValue()).getSimpleName() : entry.getValue()))
				.collect(joining(", ", "{", "}"));
	}
}
