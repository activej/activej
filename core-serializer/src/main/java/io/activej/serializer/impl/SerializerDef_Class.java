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

package io.activej.serializer.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.AbstractSerializerDef;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.serializer.util.Utils.get;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.*;
import static org.objectweb.asm.Type.*;

public final class SerializerDef_Class extends AbstractSerializerDef {

	public static final class FieldDef {
		private Field field;
		private Method method;
		private int versionAdded = -1;
		private int versionDeleted = -1;
		private SerializerDef serializer;

		public boolean hasVersion(int version) {
			if (versionAdded == -1 && versionDeleted == -1) {
				return true;
			}
			if (versionAdded != -1 && versionDeleted == -1) {
				return version >= versionAdded;
			}
			if (versionAdded == -1) {
				return version < versionDeleted;
			}
			if (versionAdded > versionDeleted) {
				return version < versionDeleted || version >= versionAdded;
			}
			if (versionAdded < versionDeleted) {
				return version >= versionAdded && version < versionDeleted;
			}
			throw new IllegalStateException("Added and deleted versions are equal");
		}

		public Class<?> getRawType() {
			if (field != null)
				return field.getType();
			if (method != null)
				return method.getReturnType();
			throw new AssertionError();
		}

		public Type getAsmType() {
			return getType(getRawType());
		}
	}

	private final Class<?> encodeType;
	private final Class<?> decodeType;

	private final LinkedHashMap<String, FieldDef> fields = new LinkedHashMap<>();

	private Constructor<?> constructor;
	private List<String> constructorParams;
	private Method staticFactoryMethod;
	private List<String> staticFactoryMethodParams;
	private final Map<Method, List<String>> setters = new LinkedHashMap<>();

	private SerializerDef_Class(Class<?> encodeType, Class<?> decodeType) {
		this.encodeType = encodeType;
		this.decodeType = decodeType;
	}

	public static SerializerDef_Class create(Class<?> type) {
		return new SerializerDef_Class(type, type);
	}

	public static SerializerDef_Class create(Class<?> encodeType, Class<?> decodeType) {
		checkArgument(!decodeType.isInterface(), "Cannot serialize an interface");
		checkArgument(encodeType.isAssignableFrom(decodeType), format("Class should be assignable from %s", decodeType));

		return new SerializerDef_Class(encodeType, decodeType);
	}

	public void addSetter(Method method, List<String> fields) {
		checkArgument(!isPrivate(method.getModifiers()), format("Setter cannot be private: %s", method));
		checkArgument(method.getGenericParameterTypes().length == fields.size(),
				"Number of arguments of a method should match a size of list of fields");
		checkArgument(!setters.containsKey(method), "Setter has already been added");

		setters.put(method, fields);
	}

	public void setStaticFactoryMethod(Method staticFactoryMethod, List<String> fields) {
		checkArgument(this.staticFactoryMethod == null, format("Factory is already set: %s", this.staticFactoryMethod));
		checkArgument(!isPrivate(staticFactoryMethod.getModifiers()), format("Factory cannot be private: %s", staticFactoryMethod));
		checkArgument(isStatic(staticFactoryMethod.getModifiers()), format("Factory must be static: %s", staticFactoryMethod));
		checkArgument(staticFactoryMethod.getGenericParameterTypes().length == fields.size(),
				"Number of arguments of a method should match a size of list of fields");

		this.staticFactoryMethod = staticFactoryMethod;
		this.staticFactoryMethodParams = fields;
	}

	public void setConstructor(Constructor<?> constructor, List<String> fields) {
		checkArgument(this.constructor == null, format("Constructor is already set: %s", this.constructor));
		checkArgument(!isPrivate(constructor.getModifiers()), format("Constructor cannot be private: %s", constructor));
		checkArgument(constructor.getGenericParameterTypes().length == fields.size(),
				"Number of arguments of a constructor should match a size of list of fields");

		this.constructor = constructor;
		this.constructorParams = fields;
	}

	public void addField(Field field, SerializerDef serializer, int added, int removed) {
		checkArgument(isPublic(field.getModifiers()), "Method should be public");

		String fieldName = field.getName();
		checkArgument(!fields.containsKey(fieldName), format("Duplicate field '%s'", field));

		FieldDef fieldDef = new FieldDef();
		fieldDef.field = field;
		fieldDef.serializer = serializer;
		fieldDef.versionAdded = added;
		fieldDef.versionDeleted = removed;
		fields.put(fieldName, fieldDef);
	}

	public void addGetter(Method method, SerializerDef serializer, int added, int removed) {
		checkArgument(method.getGenericParameterTypes().length == 0, "Method should have 0 generic parameter types");
		checkArgument(isPublic(method.getModifiers()), "Method should be public");

		String fieldName = stripGet(method.getName(), method.getReturnType());
		checkArgument(!fields.containsKey(fieldName), format("Duplicate field '%s'", method));

		FieldDef fieldDef = new FieldDef();
		fieldDef.method = method;
		fieldDef.serializer = serializer;
		fieldDef.versionAdded = added;
		fieldDef.versionDeleted = removed;
		fields.put(fieldName, fieldDef);
	}

	public void addMatchingSetters() {
		Set<String> usedFields = new HashSet<>();
		if (constructorParams != null) {
			usedFields.addAll(constructorParams);
		}
		if (staticFactoryMethodParams != null) {
			usedFields.addAll(staticFactoryMethodParams);
		}
		for (List<String> list : setters.values()) {
			usedFields.addAll(list);
		}
		for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
			String fieldName = entry.getKey();
			Method getter = entry.getValue().method;
			if (getter == null)
				continue;
			if (usedFields.contains(fieldName))
				continue;
			String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
			try {
				Method setter = decodeType.getMethod(setterName, getter.getReturnType());
				if (!isPrivate(setter.getModifiers())) {
					addSetter(setter, List.of(fieldName));
				}
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void accept(Visitor visitor) {
		for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
			visitor.visit(entry.getKey(), entry.getValue().serializer);
		}
	}

	@Override
	public Set<Integer> getVersions() {
		Set<Integer> versions = new HashSet<>();
		for (FieldDef fieldDef : fields.values()) {
			if (fieldDef.versionAdded != -1)
				versions.add(fieldDef.versionAdded);
			if (fieldDef.versionDeleted != -1)
				versions.add(fieldDef.versionDeleted);
		}
		return versions;
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return false;// fields.size() <= 1;
	}

	@Override
	public Class<?> getEncodeType() {
		return encodeType;
	}

	@Override
	public Class<?> getDecodeType() {
		return decodeType;
	}

	private static String stripGet(String getterName, Class<?> type) {
		if (type == Boolean.TYPE || type == Boolean.class) {
			if (getterName.startsWith("is") && getterName.length() > 2) {
				return Character.toLowerCase(getterName.charAt(2)) + getterName.substring(3);
			}
		}
		if (getterName.startsWith("get") && getterName.length() > 3) {
			return Character.toLowerCase(getterName.charAt(3)) + getterName.substring(4);
		}
		return getterName;
	}

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		List<Expression> list = new ArrayList<>();

		for (Map.Entry<String, FieldDef> entry : this.fields.entrySet()) {
			String fieldName = entry.getKey();
			FieldDef fieldDef = entry.getValue();
			if (!fieldDef.hasVersion(version)) continue;

			Class<?> fieldType = fieldDef.serializer.getEncodeType();

			Encoder encoder = fieldDef.serializer.defineEncoder(staticEncoders, version, compatibilityLevel);
			if (fieldDef.field != null) {
				list.add(
						encoder.encode(buf, pos, cast(property(value, fieldName), fieldType)));
			} else if (fieldDef.method != null) {
				list.add(
						encoder.encode(buf, pos, cast(call(value, fieldDef.method.getName()), fieldType)));
			} else {
				throw new AssertionError();
			}
		}

		return sequence(list);
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return decoder(staticDecoders, in, version, compatibilityLevel, value -> sequence());
	}

	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, UnaryOperator<Expression> instanceInitializer) {
		if (constructor == null && staticFactoryMethod == null && setters.isEmpty()) {
			return deserializeClassSimple(staticDecoders, in, version, compatibilityLevel, instanceInitializer);
		}

		return let(get(() -> {
					List<Expression> fieldDeserializers = new ArrayList<>();
					for (FieldDef fieldDef : fields.values()) {
						if (!fieldDef.hasVersion(version)) continue;
						fieldDeserializers.add(
								fieldDef.serializer.defineDecoder(staticDecoders, version, compatibilityLevel).decode(in));
					}
					return fieldDeserializers;
				}),
				fieldValues -> {
					Map<String, Expression> map = new HashMap<>();
					int i = 0;
					for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
						FieldDef fieldDef = entry.getValue();
						if (!fieldDef.hasVersion(version)) continue;
						map.put(entry.getKey(), fieldValues.get(i++));
					}

					return let(staticFactoryMethod == null ?
									callConstructor(decodeType, map, version) :
									callFactoryMethod(map, version),
							instance -> sequence(seq -> {
								seq.add(instanceInitializer.apply(instance));
								for (Map.Entry<Method, List<String>> entry : setters.entrySet()) {
									Method method = entry.getKey();
									List<String> fieldNames = entry.getValue();
									boolean found = false;
									for (String fieldName : fieldNames) {
										FieldDef fieldDef = fields.get(fieldName);
										if (fieldDef == null) {
											throw new NullPointerException(format("Field '%s' is not found in '%s'", fieldName, method));
										}
										if (fieldDef.hasVersion(version)) {
											found = true;
											break;
										}
									}
									if (found) {
										Class<?>[] parameterTypes = method.getParameterTypes();
										Expression[] temp = new Expression[parameterTypes.length];
										for (int j = 0; j < fieldNames.size(); j++) {
											String fieldName = fieldNames.get(j);
											FieldDef fieldDef = fields.get(fieldName);
											assert fieldDef != null;
											if (fieldDef.hasVersion(version)) {
												temp[j] = cast(map.get(fieldName), parameterTypes[j]);
											} else {
												temp[j] = cast(pushDefaultValue(fieldDef.getAsmType()), parameterTypes[j]);
											}
										}
										seq.add(call(instance, method.getName(), temp));
									}
								}

								for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
									String fieldName = entry.getKey();
									FieldDef fieldDef = entry.getValue();
									if (!fieldDef.hasVersion(version))
										continue;
									if (fieldDef.field == null || isFinal(fieldDef.field.getModifiers()))
										continue;
									Variable property = property(instance, fieldName);
									seq.add(set(property, cast(map.get(fieldName), fieldDef.getRawType())));
								}

								return instance;
							}));

				});
	}

	private Expression callFactoryMethod(Map<String, Expression> map, int version) {
		Expression[] param = extractParameters(map, version, staticFactoryMethod, staticFactoryMethodParams);
		return staticCall(staticFactoryMethod.getDeclaringClass(), staticFactoryMethod.getName(), param);
	}

	private Expression callConstructor(Class<?> targetType, Map<String, Expression> map, int version) {
		if (constructorParams == null) {
			return constructor(targetType);
		}

		Expression[] param = extractParameters(map, version, constructor, constructorParams);
		return constructor(targetType, param);
	}

	private Expression[] extractParameters(Map<String, Expression> map, int version, Executable executable, List<String> parameterNames) {
		Expression[] parameters = new Expression[parameterNames.size()];
		Class<?>[] parameterTypes = executable.getParameterTypes();
		for (int i = 0; i < parameterNames.size(); i++) {
			String fieldName = parameterNames.get(i);
			FieldDef fieldDef = fields.get(fieldName);
			if (fieldDef == null)
				throw new NullPointerException(format("Field '%s' is not found in '%s'", fieldName, executable));
			if (fieldDef.hasVersion(version)) {
				parameters[i] = cast(map.get(fieldName), parameterTypes[i]);
			} else {
				parameters[i] = cast(pushDefaultValue(fieldDef.getAsmType()), parameterTypes[i]);
			}
		}
		return parameters;
	}

	private Expression deserializeClassSimple(StaticDecoders staticDecoders, Expression in,
			int version, CompatibilityLevel compatibilityLevel, UnaryOperator<Expression> instanceInitializer) {
		return let(
				constructor(decodeType),
				instance ->
						sequence(seq -> {
							seq.add(instanceInitializer.apply(instance));
							for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
								FieldDef fieldDef = entry.getValue();
								if (!fieldDef.hasVersion(version)) continue;

								seq.add(
										set(property(instance, entry.getKey()),
												fieldDef.serializer.defineDecoder(staticDecoders, version, compatibilityLevel).decode(in)));
							}
							return instance;
						}));
	}

	private Expression pushDefaultValue(Type type) {
		return switch (type.getSort()) {
			case BOOLEAN -> value(false);
			case CHAR -> value((char) 0);
			case BYTE -> value((byte) 0);
			case SHORT -> value((short) 0);
			case INT -> value(0);
			case Type.LONG -> value(0L);
			case Type.FLOAT -> value(0f);
			case Type.DOUBLE -> value(0d);
			case ARRAY, OBJECT -> nullRef(type);
			default -> throw new IllegalArgumentException("Unsupported type " + type);
		};
	}

	@Override
	public String toString() {
		return "SerializerDefClass{" + encodeType.getSimpleName() + '}';
	}
}
