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

package io.activej.serializer.def.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.AbstractSerializerDef;
import io.activej.serializer.def.SerializerDef;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;

import java.lang.reflect.*;
import java.util.*;
import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.collection.CollectorUtils.toHashMap;
import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.objectweb.asm.Type.*;

@ExposedInternals
public final class ClassSerializerDef extends AbstractSerializerDef {

	public final Class<?> encodeType;
	public final Class<?> decodeType;

	public record PropertyDef(Member member, SerializerDef serializer, String name, int versionAdded, int versionDeleted, Object defaultValue) {
		public PropertyDef {
			checkArgument(member instanceof Method || member instanceof Field, "Property %s must be either Method or Field", member);
			checkNotNull(serializer);
			checkNotNull(name);
			checkArgument(versionAdded >= -1);
			checkArgument(versionDeleted >= -1);
		}

		public PropertyDef(Member member, SerializerDef serializer) {
			this(member, serializer, member instanceof Method method ? stripGet(method.getName(), method.getReturnType()) : ((Field) member).getName());
		}

		public PropertyDef(Member member, SerializerDef serializer, String name) {
			this(member, serializer, name, -1, -1, null);
		}

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
			if (member instanceof Field field)
				return field.getType();
			if (member instanceof Method method)
				return method.getReturnType();
			throw new AssertionError();
		}

		public Expression defaultExpression() {
			if (defaultValue != null) return Expressions.value(defaultValue, getRawType());
			Type asmType = getType(getRawType());
			return switch (asmType.getSort()) {
				case BOOLEAN -> value(false);
				case CHAR -> value((char) 0);
				case BYTE -> value((byte) 0);
				case SHORT -> value((short) 0);
				case INT -> value(0);
				case Type.LONG -> value(0L);
				case Type.FLOAT -> value(0f);
				case Type.DOUBLE -> value(0d);
				case ARRAY, OBJECT -> nullRef(asmType);
				default -> throw new IllegalArgumentException("Unsupported type " + asmType);
			};
		}
	}

	public final List<PropertyDef> properties;

	public record SetterDef(Method method, List<String> properties) {}

	public final List<SetterDef> setters;

	public record FactoryDef(Executable member, List<String> properties) {}

	public @Nullable FactoryDef factory;

	public ClassSerializerDef(
		Class<?> encodeType, Class<?> decodeType,
		List<PropertyDef> properties, List<SetterDef> setters, @Nullable FactoryDef factory
	) {
		var propertyNames = properties.stream().map(PropertyDef::name).collect(toSet());
		checkArgument(setters.stream().allMatch(setter -> propertyNames.containsAll(setter.properties)));
		checkArgument(factory == null || propertyNames.containsAll(factory.properties));
		this.encodeType = encodeType;
		this.decodeType = decodeType;
		this.properties = properties;
		this.setters = setters;
		this.factory = factory;
	}

	public static Builder builder(Class<?> type) {
		return builder(type, type);
	}

	public static Builder builder(Class<?> encodeType, Class<?> decodeType) {
		checkArgument(!decodeType.isInterface(), "Cannot serialize an interface " + decodeType.getName());
		checkArgument(encodeType.isAssignableFrom(decodeType), format("Class %s should be assignable from %s", encodeType, decodeType));

		return new ClassSerializerDef(encodeType, decodeType, new ArrayList<>(), new ArrayList<>(), null).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ClassSerializerDef> {
		private Builder() {}

		public Builder withField(Field field, SerializerDef serializer, int added, int removed) {
			checkNotBuilt(this);
			checkArgument(isPublic(field.getModifiers()), "Field %s should be public", field);
			ClassSerializerDef.this.properties.removeIf(property ->
				property.member instanceof Field f &&
				Objects.equals(f.getName(), field.getName()));
			String name = field.getName();
			PropertyDef propertyDef = new PropertyDef(field, serializer, name, added, removed, null);
			return withPropertyDef(propertyDef);
		}

		public Builder withGetter(Method method, SerializerDef serializer, int added, int removed) {
			checkNotBuilt(this);
			checkArgument(method.getGenericParameterTypes().length == 0, "Method %s should have 0 generic parameter types", method);
			checkArgument(isPublic(method.getModifiers()), "Method %s should be public", method);
			ClassSerializerDef.this.properties.removeIf(property ->
				property.member instanceof Method m &&
				Objects.equals(m.getName(), method.getName()) &&
				Arrays.equals(m.getParameterTypes(), method.getParameterTypes()));
			String name = stripGet(method.getName(), method.getReturnType());
			return withPropertyDef(new PropertyDef(method, serializer, name, added, removed, null));
		}

		public Builder withPropertyDef(PropertyDef propertyDef) {
			properties.add(propertyDef);
			return this;
		}

		public Builder withSetter(Method method, List<String> properties) {
			checkNotBuilt(this);
			checkArgument(!isPrivate(method.getModifiers()), format("Setter cannot be private: %s", method));
			checkArgument(method.getGenericParameterTypes().length == properties.size(),
				"Number of arguments of a method %s should match a size of list of properties (%d)", method, properties.size());
			ClassSerializerDef.this.setters.removeIf(setter ->
				Objects.equals(setter.method.getName(), method.getName()) &&
				Arrays.equals(setter.method.getParameterTypes(), method.getParameterTypes()));
			return withSetterDef(new SetterDef(method, properties));
		}

		public Builder withSetterDef(SetterDef setterDef) {
			ClassSerializerDef.this.setters.add(setterDef);
			return this;
		}

		public Builder withConstructor(Constructor<?> constructor, List<String> properties) {
			checkNotBuilt(this);
			checkArgument(!isPrivate(constructor.getModifiers()), format("Constructor cannot be private: %s", constructor));
			checkArgument(constructor.getGenericParameterTypes().length == properties.size(),
				"Number of arguments of a constructor %s should match a size of list of properties (%d)", constructor, properties.size());
			return withFactoryDef(new FactoryDef(constructor, properties));
		}

		public Builder withStaticFactoryMethod(Method staticFactoryMethod, List<String> properties) {
			checkNotBuilt(this);
			checkArgument(!isPrivate(staticFactoryMethod.getModifiers()), format("Factory cannot be private: %s", staticFactoryMethod));
			checkArgument(isStatic(staticFactoryMethod.getModifiers()), format("Factory must be static: %s", staticFactoryMethod));
			checkArgument(staticFactoryMethod.getGenericParameterTypes().length == properties.size(),
				"Number of arguments of a method %s should match a size of list of properties (%d)", staticFactoryMethod, properties.size());
			return withFactoryDef(new FactoryDef(staticFactoryMethod, properties));
		}

		public Builder withFactoryDef(FactoryDef factoryDef) {
			ClassSerializerDef.this.factory = factoryDef;
			return this;
		}

		public Builder withMatchingSetters() {
			checkNotBuilt(this);
			Set<String> usedProperties = new HashSet<>();
			if (factory != null) {
				usedProperties.addAll(factory.properties);
			}
			for (var setter : setters) {
				usedProperties.addAll(setter.properties);
			}
			for (var property : properties) {
				if (!(property.member instanceof Method getter)) continue;
				if (usedProperties.contains(property.name)) continue;
				String setterName = "set" + toUpperCase(property.name.charAt(0)) + property.name.substring(1);
				try {
					Method setter = decodeType.getMethod(setterName, getter.getReturnType());
					if (!isPrivate(setter.getModifiers())) {
						withSetter(setter, List.of(property.name));
					}
				} catch (NoSuchMethodException e) {
					throw new IllegalArgumentException(e);
				}
			}
			return this;
		}

		@Override
		protected ClassSerializerDef doBuild() {
			return ClassSerializerDef.this;
		}
	}

	@Override
	public void accept(Visitor visitor) {
		for (var property : properties) {
			visitor.visit(property.name, property.serializer);
		}
	}

	@Override
	public Set<Integer> getVersions() {
		Set<Integer> versions = new HashSet<>();
		for (var property : properties) {
			if (property.versionAdded != -1) {
				versions.add(property.versionAdded);
			}
			if (property.versionDeleted != -1) {
				versions.add(property.versionDeleted);
			}
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

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		List<Expression> list = new ArrayList<>();
		for (var property : properties) {
			if (!property.hasVersion(version)) continue;
			Encoder encoder = property.serializer.defineEncoder(staticEncoders, version, compatibilityLevel);
			Expression expression;
			Class<?> fieldType = property.serializer.getEncodeType();
			if (property.member instanceof Field field) {
				expression = encoder.encode(buf, pos, cast(property(value, field.getName()), fieldType));
			} else if (property.member instanceof Method method) {
				expression = encoder.encode(buf, pos, cast(call(value, method.getName()), fieldType));
			} else {
				throw new AssertionError();
			}
			list.add(expression);
		}
		return sequence(list);
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return decoder(staticDecoders, in, version, compatibilityLevel, value -> sequence());
	}

	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, UnaryOperator<Expression> instanceInitializer) {
		Map<String, PropertyDef> propertyMap = properties.stream().collect(toHashMap(p -> p.name, identity()));

		return let(
			propertyExpressions -> {
				for (PropertyDef propertyDef : properties) {
					if (!propertyDef.hasVersion(version)) continue;
					propertyExpressions.add(
						propertyDef.serializer.defineDecoder(staticDecoders, version, compatibilityLevel).decode(in));
				}
			},
			propertyValues -> {

				Map<String, Expression> propertyValuesMap = new HashMap<>();
				int i = 0;
				for (var propertyDef : properties) {
					if (!propertyDef.hasVersion(version)) continue;
					propertyValuesMap.put(propertyDef.name, propertyValues[i++]);
				}

				return let(
					factory == null ?
						constructor(decodeType) :
						factory.member instanceof Constructor ?
							constructor(decodeType,
								extractParameters(factory.member, factory.properties, version, propertyMap, propertyValuesMap)) :
							staticCall(factory.member.getDeclaringClass(), factory.member.getName(),
								extractParameters(factory.member, factory.properties, version, propertyMap, propertyValuesMap)),
					instance -> sequence(seq -> {
						seq.add(instanceInitializer.apply(instance));
						for (var setter : setters) {
							if (setter.properties.stream().map(propertyMap::get).noneMatch(propertyDef -> propertyDef.hasVersion(version))) {
								continue;
							}
							seq.add(
								call(instance, setter.method.getName(),
									extractParameters(setter.method, setter.properties, version, propertyMap, propertyValuesMap)));
						}

						for (var propertyDef : properties) {
							if (!propertyDef.hasVersion(version)) continue;
							if (!(propertyDef.member instanceof Field field)) continue;
							if (isFinal(field.getModifiers())) continue;
							seq.add(
								set(property(instance, field.getName()),
									cast(propertyValuesMap.get(propertyDef.name), propertyDef.getRawType())));
						}

						return instance;
					}));

			});
	}

	private static Expression[] extractParameters(
		Executable executable, List<String> properties, int version,
		Map<String, PropertyDef> propertyMap, Map<String, Expression> propertyValuesMap
	) {
		Expression[] parameters = new Expression[properties.size()];
		for (int i = 0; i < properties.size(); i++) {
			String property = properties.get(i);
			PropertyDef propertyDef = propertyMap.get(property);
			if (propertyDef == null)
				throw new NullPointerException(format("Field '%s' is not found in '%s'", property, executable));
			if (propertyDef.hasVersion(version)) {
				parameters[i] = cast(propertyValuesMap.get(property), executable.getParameterTypes()[i]);
			} else {
				parameters[i] = cast(propertyDef.defaultExpression(), executable.getParameterTypes()[i]);
			}
		}
		return parameters;
	}

	private static String stripGet(String getterName, Class<?> type) {
		if (type == Boolean.TYPE || type == BooleanSerializerDef.class) {
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
	public String toString() {
		return "ClassSerializerDef{" + encodeType.getSimpleName() + '}';
	}
}
