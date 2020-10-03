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
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;
import io.activej.codegen.util.WithInitializer;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.*;

import static io.activej.codegen.expression.Expressions.*;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unused", "ArraysAsListWithZeroOrOneArgument"})
public final class RecordScheme implements WithInitializer<RecordScheme> {
	private RecordFactory factory;

	protected RecordGetter[] recordGetters;
	protected RecordSetter[] recordSetters;

	protected final HashMap<String, RecordGetter> recordGettersMap = new HashMap<>();
	protected final HashMap<String, RecordSetter> recordSettersMap = new HashMap<>();

	protected final LinkedHashMap<String, Type> fieldTypes = new LinkedHashMap<>();
	protected final LinkedHashMap<String, Integer> fieldIndices = new LinkedHashMap<>();
	protected String[] fields = {};
	protected Type[] types = {};
	protected final HashMap<String, String> classFields = new HashMap<>();

	@NotNull
	private final DefiningClassLoader classLoader;
	private Class<? extends Record> generatedClass;

	private RecordScheme(@NotNull DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public static RecordScheme create() {
		return new RecordScheme(DefiningClassLoader.create());
	}

	public static RecordScheme create(@NotNull DefiningClassLoader classLoader) {
		return new RecordScheme(classLoader);
	}

	@SuppressWarnings("UnusedReturnValue")
	public RecordScheme withField(@NotNull String field, @NotNull Type type) {
		addField(field, type);
		return this;
	}

	public void addField(@NotNull String field, @NotNull Type type) {
		if (factory != null) throw new IllegalStateException("Already initialized");
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
	}

	public void addFields(Map<String, Class<?>> types) {
		for (Map.Entry<String, Class<?>> entry : types.entrySet()) {
			addField(entry.getKey(), entry.getValue());
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

	public @NotNull DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	public Class<? extends Record> getRecordClass() {
		build();
		return generatedClass;
	}

	public String getClassField(String field) {
		return classFields.get(field);
	}

	public Variable property(Expression record, String field) {
		return Expressions.property(record, getClassField(field));
	}

	public List<String> getFields() {
		return new ArrayList<>(fieldTypes.keySet());
	}

	public String getField(int index) {
		return fields[index];
	}

	public Type getFieldType(String field) {
		return fieldTypes.get(field);
	}

	public int getFieldIndex(String field) {
		return fieldIndices.get(field);
	}

	public int size() {
		return fields.length;
	}

	public RecordScheme build() {
		if (generatedClass == null) {
			doEnsureBuild();
		}
		return this;
	}

	synchronized private void doEnsureBuild() {
		ClassBuilder<Record> builder = ClassBuilder.create(this.classLoader, Record.class)
				.withClassKey(this)
//				.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath())
				.withMethod("<init>", void.class, asList(RecordScheme.class),
						callSuper(Record.class, arg(0)))
				.withMethod("hashCode",
						hash(Arrays.stream(fields).map(this::getClassField).map(f -> Expressions.property(self(), f)).collect(toList())))
				.withMethod("equals",
						equalsImpl(Arrays.stream(fields).map(this::getClassField).collect(toList())))
						;
		for (String field : fieldTypes.keySet()) {
			Type type = fieldTypes.get(field);
			builder.withField(getClassField(field), type instanceof Class ? ((Class) type) : Object.class);
		}
		generatedClass = builder.build();

		recordGetters = new RecordGetter[size()];
		recordSetters = new RecordSetter[size()];
		for (String field : fieldTypes.keySet()) {
			Type fieldType = fieldTypes.get(field);
			Variable property = this.property(cast(arg(0), generatedClass), field);
			RecordGetter recordGetter = ClassBuilder.create(this.classLoader, RecordGetter.class)
					.withClassKey(this, field)
//					.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath())
					.withMethod("get", property)
					.withInitializer(cb -> {
						if (fieldType == byte.class || fieldType == short.class || fieldType == int.class || fieldType == long.class || fieldType == float.class || fieldType == double.class ||
								fieldType == Byte.class || fieldType == Short.class || fieldType == Integer.class || fieldType == Long.class || fieldType == Float.class || fieldType == Double.class) {
							cb.withMethod("getInt", property);
							cb.withMethod("getLong", property);
							cb.withMethod("getFloat", property);
							cb.withMethod("getDouble", property);
						}
					})
					.withMethod("getScheme", value(this))
					.withMethod("getField", value(field))
					.withMethod("getType", value(fieldType))
					.buildClassAndCreateNewInstance();
			recordGetters[recordGettersMap.size()] = recordGetter;
			recordGettersMap.put(field, recordGetter);

			Expression set = Expressions.set(property, arg(1));
			RecordSetter recordSetter = ClassBuilder.create(this.classLoader, RecordSetter.class)
					.withClassKey(this, field)
//					.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath())
					.withMethod("set", set)
					.withInitializer(cb -> {
						if (fieldType == byte.class || fieldType == short.class || fieldType == int.class || fieldType == long.class || fieldType == float.class || fieldType == double.class ||
								fieldType == Byte.class || fieldType == Short.class || fieldType == Integer.class || fieldType == Long.class || fieldType == Float.class || fieldType == Double.class) {
							cb.withMethod("setInt", set);
							cb.withMethod("setLong", set);
							cb.withMethod("setFloat", set);
							cb.withMethod("setDouble", set);
						}
					})
					.withMethod("getScheme", value(this))
					.withMethod("getField", value(field))
					.withMethod("getType", value(fieldType))
					.buildClassAndCreateNewInstance();
			recordSetters[recordSettersMap.size()] = recordSetter;
			recordSettersMap.put(field, recordSetter);
		}

		factory = ClassBuilder.create(this.classLoader, RecordFactory.class)
				.withClassKey(this)
//				.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath())
				.withStaticFinalField("SCHEME", RecordScheme.class, value(this))
				.withMethod("create", Record.class, asList(),
						constructor(generatedClass, staticField("SCHEME")))
				.buildClassAndCreateNewInstance();
	}

	public RecordGetter getter(String field) {
		return recordGettersMap.get(field);
	}

	public RecordGetter getter(int field) {
		return recordGetters[field];
	}

	public Object get(Record record, String field) {
		return getter(field).get(record);
	}

	public Object get(Record record, int field) {
		return getter(field).get(record);
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

	public RecordSetter setter(String field) {
		return recordSettersMap.get(field);
	}

	public RecordSetter setter(int field) {
		return recordSetters[field];
	}

	public void set(Record record, String field, Object value) {
		setter(field).set(record, value);
	}

	public void set(Record record, int field, Object value) {
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
