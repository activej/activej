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
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("unused")
public final class RecordScheme {
	private RecordFactory factory;

	private RecordGetter<?>[] recordGetters;
	private RecordSetter<?>[] recordSetters;

	private @Nullable Comparator<Record> comparator;

	private final HashMap<String, RecordGetter<?>> recordGettersMap = new HashMap<>();
	private final HashMap<String, RecordSetter<?>> recordSettersMap = new HashMap<>();

	private final LinkedHashMap<String, Type> fieldTypes = new LinkedHashMap<>();
	private final LinkedHashMap<String, Integer> fieldIndices = new LinkedHashMap<>();
	private Type[] types = {};
	private final HashMap<String, String> classFields = new HashMap<>();

	String[] fields = {};

	private @Nullable List<String> hashCodeEqualsFields;

	private @Nullable List<String> comparedFields;

	private final DefiningClassLoader classLoader;
	private Class<? extends Record> generatedClass;

	private RecordScheme(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public static Builder builder() {
		return builder(DefiningClassLoader.create());
	}

	public static Builder builder(DefiningClassLoader classLoader) {
		return new RecordScheme(classLoader).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RecordScheme> {
		private Builder() {}

		@SuppressWarnings("UnusedReturnValue")
		public Builder withField(String field, Type type) {
			checkNotBuilt(this);
			if (fieldTypes.containsKey(field)) throw new IllegalArgumentException("Duplicate field");
			fieldTypes.put(field, type);
			fields = Arrays.copyOf(fields, fields.length + 1);
			fields[fields.length - 1] = field;
			types = Arrays.copyOf(types, types.length + 1);
			types[types.length - 1] = type;
			fieldIndices.put(field, fieldIndices.size());

			char[] chars = (Character.isJavaIdentifierStart(field.charAt(0)) ? field : "_" + field).toCharArray();
			for (int i = 1; i < chars.length; i++) {
				if (!Character.isJavaIdentifierPart(chars[i])) {
					chars[i] = '_';
				}
			}
			String sanitized = new String(chars);

			for (int i = 1; ; i++) {
				String classField = i == 1 ? sanitized : sanitized + i;
				if (!classFields.containsKey(classField)) {
					classFields.put(field, classField);
					break;
				}
			}
			return this;
		}

		public Builder withHashCodeEqualsFields(List<String> hashCodeEqualsFields) {
			checkNotBuilt(this);
			checkUnique(hashCodeEqualsFields);
			RecordScheme.this.hashCodeEqualsFields = hashCodeEqualsFields;
			return this;
		}

		public Builder withHashCodeEqualsFields(String... hashCodeEqualsFields) {
			checkNotBuilt(this);
			return withHashCodeEqualsFields(List.of(hashCodeEqualsFields));
		}

		public Builder withComparator(List<String> comparedFields) {
			checkNotBuilt(this);
			checkUnique(comparedFields);
			RecordScheme.this.comparedFields = comparedFields;
			return this;
		}

		public Builder withComparator(String... comparedFields) {
			checkNotBuilt(this);
			return withComparator(List.of(comparedFields));
		}

		@Override
		protected RecordScheme doBuild() {
			Collection<String> hashCodeEqualsFields1;
			if (RecordScheme.this.hashCodeEqualsFields != null) {
				Set<String> missing = getMissingFields(RecordScheme.this.hashCodeEqualsFields);
				if (!missing.isEmpty()) {
					throw new IllegalStateException("Missing some fields to generate 'hashCode' and 'equals' methods: " + missing);
				}
				hashCodeEqualsFields1 = RecordScheme.this.hashCodeEqualsFields;
			} else {
				hashCodeEqualsFields1 = fieldTypes.keySet();
			}
			List<String> hashCodeEqualsClassFields = hashCodeEqualsFields1.stream()
					.map(RecordScheme.this::getClassField)
					.collect(Collectors.toList());
			generatedClass = classLoader.ensureClass(
					ClassKey.of(Record.class, RecordScheme.this),
					() -> ClassBuilder.create(Record.class)
							.withConstructor(List.of(RecordScheme.class),
									superConstructor(arg(0)))
							.withMethod("hashCode", hashCodeImpl(hashCodeEqualsClassFields))
							.withMethod("equals", equalsImpl(hashCodeEqualsClassFields))
							.initialize(b -> {
								for (Map.Entry<String, Type> entry : fieldTypes.entrySet()) {
									Type type = entry.getValue();
									//noinspection rawtypes
									b.withField(getClassField(entry.getKey()), type instanceof Class ? ((Class) type) : Object.class);
								}
							}));
			recordGetters = new RecordGetter[size()];
			recordSetters = new RecordSetter[size()];
			for (Map.Entry<String, Type> entry : fieldTypes.entrySet()) {
				String field = entry.getKey();
				Type fieldType = entry.getValue();
				Variable property = RecordScheme.this.property(cast(arg(0), generatedClass), field);
				RecordGetter<?> recordGetter = classLoader.ensureClassAndCreateInstance(
						ClassKey.of(RecordGetter.class, RecordScheme.this, field),
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
								.withMethod("getScheme", value(RecordScheme.this))
								.withMethod("getField", value(field))
								.withMethod("getType", value(fieldType, Type.class))
				);
				recordGetters[recordGettersMap.size()] = recordGetter;
				recordGettersMap.put(field, recordGetter);

				Expression set = Expression.set(property, arg(1));
				RecordSetter<?> recordSetter = classLoader.ensureClassAndCreateInstance(
						ClassKey.of(RecordSetter.class, RecordScheme.this, field),
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
								.withMethod("getScheme", value(RecordScheme.this))
								.withMethod("getField", value(field))
								.withMethod("getType", value(fieldType, Type.class)));
				recordSetters[recordSettersMap.size()] = recordSetter;
				recordSettersMap.put(field, recordSetter);
			}
			if (comparedFields != null) {
				Set<String> missing = getMissingFields(comparedFields);
				if (!missing.isEmpty()) {
					throw new IllegalStateException("Missing some fields to be compared: " + missing);
				}

				List<String> comparedClassFields = comparedFields.stream()
						.map(RecordScheme.this::getClassField)
						.collect(Collectors.toList());

				//noinspection unchecked
				comparator = ClassBuilder.create(Comparator.class)
						.withMethod("compare", comparatorImpl(generatedClass, comparedClassFields))
						.defineClassAndCreateInstance(classLoader);
			}
			factory = classLoader.ensureClassAndCreateInstance(
					ClassKey.of(RecordFactory.class, RecordScheme.this),
					() -> ClassBuilder.create(RecordFactory.class)
							.withStaticFinalField("SCHEME", RecordScheme.class, value(RecordScheme.this))
							.withMethod("create", Record.class, List.of(),
									constructor(generatedClass, staticField("SCHEME"))));

			return RecordScheme.this;
		}
	}

	public Record record() {
		return factory.create();
	}

	public Comparator<Record> recordComparator() {
		if (comparator == null) throw new IllegalStateException("Compared fields were not specified");

		return comparator;
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
		return generatedClass;
	}

	public String getClassField(String field) {
		return classFields.get(field);
	}

	public Variable property(Expression record, String field) {
		return Expression.property(record, getClassField(field));
	}

	public List<String> getFields() {
		return new ArrayList<>(fieldTypes.keySet());
	}

	public List<Type> getTypes() {
		return new ArrayList<>(fieldTypes.values());
	}

	public String getField(int index) {
		return fields[index];
	}

	public Type getFieldType(String field) {
		return fieldTypes.get(field);
	}

	public Type getFieldType(int field) {
		return types[field];
	}

	public int getFieldIndex(String field) {
		return fieldIndices.get(field);
	}

	public List<String> getComparedFields() {
		return comparedFields == null ? Collections.emptyList() : new ArrayList<>(comparedFields);
	}

	public int size() {
		return fields.length;
	}

	private static boolean isImplicitType(Type fieldType) {
		return fieldType == byte.class || fieldType == short.class || fieldType == int.class || fieldType == long.class || fieldType == float.class || fieldType == double.class ||
				fieldType == Byte.class || fieldType == Short.class || fieldType == Integer.class || fieldType == Long.class || fieldType == Float.class || fieldType == Double.class;
	}

	private Set<String> getMissingFields(List<String> fields) {
		return fields.stream()
				.filter(field -> !fieldTypes.containsKey(field))
				.collect(toSet());
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
				Arrays.equals(types, scheme.types);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fields);
		result = 31 * result + Arrays.hashCode(types);
		return result;
	}

	@Override
	public String toString() {
		return fieldTypes.entrySet().stream()
				.map(entry -> entry.getKey() + "=" +
						(entry.getValue() instanceof Class ? ((Class<?>) entry.getValue()).getSimpleName() : entry.getValue()))
				.collect(joining(", ", "{", "}"));
	}
}
