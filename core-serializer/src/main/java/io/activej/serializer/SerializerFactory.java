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

package io.activej.serializer;

import io.activej.codegen.ClassGenerator;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;
import io.activej.common.builder.AbstractBuilder;
import io.activej.serializer.annotations.*;
import io.activej.serializer.def.*;
import io.activej.serializer.def.impl.ClassSerializerDef;
import io.activej.serializer.def.impl.SubclassSerializerDef;
import io.activej.types.AnnotationUtils;
import io.activej.types.TypeT;
import io.activej.types.scanner.TypeScannerRegistry;
import io.activej.types.scanner.TypeScannerRegistry.Context;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.*;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.lang.reflect.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;
import java.util.function.Function;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.def.SerializerDef.*;
import static io.activej.serializer.def.SerializerExpressions.readByte;
import static io.activej.serializer.def.SerializerExpressions.writeByte;
import static io.activej.types.AnnotatedTypes.*;
import static io.activej.types.Utils.getAnnotation;
import static io.activej.types.Utils.hasAnnotation;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.lang.reflect.Modifier.*;
import static java.util.Collections.newSetFromMap;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.objectweb.asm.ClassReader.*;
import static org.objectweb.asm.Type.getType;

/**
 * Scans fields of classes for serialization.
 */
public final class SerializerFactory {
	private final TypeScannerRegistry<SerializerDef> registry = TypeScannerRegistry.create();

	private Class<?> implementationClass = Object.class;

	private String profile;
	private int encodeVersionMax = Integer.MAX_VALUE;
	private int decodeVersionMin = 0;
	private int decodeVersionMax = Integer.MAX_VALUE;
	private CompatibilityLevel compatibilityLevel = CompatibilityLevel.LEVEL_4;

	private final Map<Object, List<Class<?>>> extraSubclassesMap = new HashMap<>();

	private SerializerFactory() {
	}

	/**
	 * Creates a new instance of {@link  SerializerFactory}
	 */
	public static SerializerFactory defaultInstance() {
		return builder().build();
	}

	/**
	 * Creates a builder of {@link  SerializerFactory}
	 */
	public static Builder builder() {
		SerializerFactory factory = new SerializerFactory();
		return factory.new Builder()
			.with(boolean.class, ctx -> SerializerDefs.ofBoolean(false))
			.with(char.class, ctx -> SerializerDefs.ofChar(false))
			.with(byte.class, ctx -> SerializerDefs.ofByte(false))
			.with(short.class, ctx -> SerializerDefs.ofShort(false))
			.with(int.class, ctx -> SerializerDefs.ofInt(false))
			.with(long.class, ctx -> SerializerDefs.ofLong(false))
			.with(float.class, ctx -> SerializerDefs.ofFloat(false))
			.with(double.class, ctx -> SerializerDefs.ofDouble(false))

			.with(Boolean.class, ctx -> SerializerDefs.ofBoolean(true))
			.with(Character.class, ctx -> SerializerDefs.ofChar(true))
			.with(Byte.class, ctx -> SerializerDefs.ofByte(true))
			.with(Short.class, ctx -> SerializerDefs.ofShort(true))
			.with(Integer.class, ctx -> SerializerDefs.ofInt(true))
			.with(Long.class, ctx -> SerializerDefs.ofLong(true))
			.with(Float.class, ctx -> SerializerDefs.ofFloat(true))
			.with(Double.class, ctx -> SerializerDefs.ofDouble(true))

			.initialize(builder -> {
				for (Type type : new Type[]{
					boolean[].class, char[].class, byte[].class, short[].class, int[].class, long[].class, float[].class, double[].class,
					Object[].class}) {
					builder.with(type, ctx -> SerializerDefs.ofArray(ctx.scanTypeArgument(0)));
				}
			})

			.with(Inet4Address.class, ctx -> SerializerDefs.ofInet4Address())
			.with(Inet6Address.class, ctx -> SerializerDefs.ofInet6Address())
			.with(InetAddress.class, ctx -> SerializerDefs.ofInetAddress())

			.with(Enum.class, ctx -> {
				for (Method method : ctx.getRawType().getDeclaredMethods()) {
					if (AnnotationUtils.hasAnnotation(Serialize.class, method.getAnnotations())) {
						return factory.scan(ctx);
					}
				}
				for (Field field : ctx.getRawType().getDeclaredFields()) {
					if (AnnotationUtils.hasAnnotation(Serialize.class, field.getAnnotations())) {
						return factory.scan(ctx);
					}
				}
				//noinspection unchecked,rawtypes
				return SerializerDefs.ofEnum((Class<Enum>) ctx.getRawType());
			})

			.with(String.class, ctx -> {
				SerializeStringFormat a = getAnnotation(ctx.getAnnotations(), SerializeStringFormat.class);
				return SerializerDefs.ofString(a == null ? StringFormat.UTF8 : a.value());
			})

			.with(Collection.class, ctx -> SerializerDefs.ofCollection(ctx.scanTypeArgument(0), Collection.class, ArrayList.class))
			.with(Queue.class, ctx -> SerializerDefs.ofCollection(ctx.scanTypeArgument(0), Queue.class, ArrayDeque.class))

			.with(List.class, ctx -> SerializerDefs.ofList(ctx.scanTypeArgument(0)))
			.with(ArrayList.class, ctx -> SerializerDefs.ofCollection(ctx.scanTypeArgument(0), ArrayList.class, ArrayList.class))
			.with(LinkedList.class, ctx -> SerializerDefs.ofLinkedList(ctx.scanTypeArgument(0)))

			.with(Map.class, ctx -> SerializerDefs.ofMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1)))
			.with(HashMap.class, ctx -> SerializerDefs.ofHashMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1), HashMap.class, HashMap.class))
			.with(LinkedHashMap.class, ctx -> SerializerDefs.ofHashMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1), LinkedHashMap.class, LinkedHashMap.class))
			.with(EnumMap.class, ctx -> SerializerDefs.ofEnumMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1)))

			.with(Set.class, ctx -> SerializerDefs.ofSet(ctx.scanTypeArgument(0)))
			.with(HashSet.class, ctx -> SerializerDefs.ofHashSet(ctx.scanTypeArgument(0), HashSet.class, HashSet.class))
			.with(LinkedHashSet.class, ctx -> SerializerDefs.ofHashSet(ctx.scanTypeArgument(0), LinkedHashSet.class, LinkedHashSet.class))
			.with(EnumSet.class, ctx -> SerializerDefs.ofEnumSet(ctx.scanTypeArgument(0)))

			.with(Object.class, factory::scan);
	}

	public final class Builder extends AbstractBuilder<Builder, SerializerFactory> {
		private Builder() {}

		/**
		 * Adds a mapping to resolve a {@link SerializerDef} for a given {@link TypeT}
		 *
		 * @param typeT a type token
		 * @param fn    a mapping to resolve a serializer
		 */
		public Builder with(TypeT<?> typeT, TypeScannerRegistry.Mapping<SerializerDef> fn) {
			checkNotBuilt(this);
			return with(typeT.getType(), fn);
		}

		/**
		 * Adds a mapping to resolve a {@link SerializerDef} for a given {@link Type}
		 *
		 * @param type a type
		 * @param fn   a mapping to resolve a serializer
		 */
		@SuppressWarnings("PointlessBooleanExpression")
		public Builder with(Type type, TypeScannerRegistry.Mapping<SerializerDef> fn) {
			checkNotBuilt(this);
			registry.with(type, ctx -> {
				Class<?> rawClass = ctx.getRawType();
				SerializerDef serializerDef;
				SerializeClass annotationClass;
				if (false ||
					(annotationClass = getAnnotation(ctx.getAnnotations(), SerializeClass.class)) != null ||
					(annotationClass = getAnnotation(rawClass.getAnnotations(), SerializeClass.class)) != null) {
					if (annotationClass.value() != SerializerDef.class) {
						try {
							serializerDef = annotationClass.value().getDeclaredConstructor().newInstance();
						} catch (
							InstantiationException |
							IllegalAccessException |
							NoSuchMethodException |
							InvocationTargetException e
						) {
							throw new RuntimeException(e);
						}
					} else {
						SubclassSerializerDef.Builder subclassBuilder = SubclassSerializerDef.builder(rawClass);
						LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(List.of(annotationClass.subclasses()));
						subclassesSet.addAll(extraSubclassesMap.getOrDefault(rawClass, List.of()));
						subclassesSet.addAll(extraSubclassesMap.getOrDefault(annotationClass.subclassesId(), List.of()));
						for (Class<?> subclass : subclassesSet) {
							subclassBuilder.withSubclass(subclass, ctx.scan(subclass));
						}
						serializerDef = subclassBuilder
							.withStartIndex(annotationClass.subclassesIdx())
							.build();
					}
				} else if (extraSubclassesMap.containsKey(rawClass)) {
					SubclassSerializerDef.Builder subclassBuilder = SubclassSerializerDef.builder(rawClass);
					for (Class<?> subclass : extraSubclassesMap.get(rawClass)) {
						subclassBuilder.withSubclass(subclass, ctx.scan(subclass));
					}
					serializerDef = subclassBuilder.build();
				} else {
					serializerDef = fn.apply(ctx);
				}

				if (hasAnnotation(ctx.getAnnotations(), SerializeVarLength.class)) {
					serializerDef = ((SerializerDefWithVarLength) serializerDef).ensureVarLength();
				}

				SerializeFixedSize annotationFixedSize;
				if ((annotationFixedSize = getAnnotation(ctx.getAnnotations(), SerializeFixedSize.class)) != null) {
					serializerDef = ((SerializerDefWithFixedSize) serializerDef).ensureFixedSize(annotationFixedSize.value());
				}

				if (hasAnnotation(ctx.getAnnotations(), SerializeNullable.class)) {
					serializerDef = serializerDef instanceof SerializerDefWithNullable ?
						((SerializerDefWithNullable) serializerDef).ensureNullable(compatibilityLevel) : SerializerDefs.ofNullable(serializerDef);
				}

				return serializerDef;
			});
			return this;
		}

		/**
		 * Adds an implementation class for the serializer
		 *
		 * @param implementationClass an implementation class
		 */
		public Builder withImplementationClass(Class<?> implementationClass) {
			checkNotBuilt(this);
			SerializerFactory.this.implementationClass = implementationClass;
			return this;
		}

		/**
		 * Sets a given {@link CompatibilityLevel} for the serializer. This method should be used
		 * to ensure backwards compatibility with previous versions of serializers
		 *
		 * @param compatibilityLevel a compatibility level
		 */
		public Builder withCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
			checkNotBuilt(this);
			SerializerFactory.this.compatibilityLevel = compatibilityLevel;
			return this;
		}

		/**
		 * Sets maximal encode version
		 * <p>
		 * This method is used to ensure compatibility between different versions of serialized objects
		 *
		 * @param encodeVersionMax a maximal encode version
		 */
		public Builder withEncodeVersion(int encodeVersionMax) {
			checkNotBuilt(this);
			SerializerFactory.this.encodeVersionMax = encodeVersionMax;
			return this;
		}

		/**
		 * Sets both minimal and maximal decode versions
		 *
		 * <p>
		 * This method is used to ensure compatibility between different versions of serialized objects
		 *
		 * @param decodeVersionMin a minimal decode version
		 * @param decodeVersionMax a maximal decode version
		 */
		public Builder withDecodeVersions(int decodeVersionMin, int decodeVersionMax) {
			checkNotBuilt(this);
			SerializerFactory.this.decodeVersionMin = decodeVersionMin;
			SerializerFactory.this.decodeVersionMax = decodeVersionMax;
			return this;
		}

		/**
		 * Sets maximal encode version as well as both minimal and maximal decode versions
		 *
		 * <p>
		 * This method is used to ensure compatibility between different versions of serialized objects
		 *
		 * @param encodeVersionMax a maximal encode version
		 * @param decodeVersionMin a minimal decode version
		 * @param decodeVersionMax a maximal decode version
		 */
		public Builder withVersions(int encodeVersionMax, int decodeVersionMin, int decodeVersionMax) {
			checkNotBuilt(this);
			SerializerFactory.this.encodeVersionMax = encodeVersionMax;
			SerializerFactory.this.decodeVersionMin = decodeVersionMin;
			SerializerFactory.this.decodeVersionMax = decodeVersionMax;
			return this;
		}

		/**
		 * Sets a serializer profile
		 *
		 * @param profile a serializer profile
		 */
		public Builder withProfile(String profile) {
			checkNotBuilt(this);
			SerializerFactory.this.profile = profile;
			return this;
		}

		/**
		 * Sets subclasses to be serialized.
		 * Uses custom string id to identify subclasses
		 * <p>
		 * <b>Order of subclasses matters. To keep serializers compatible, the order of subclasses should not change</b>
		 *
		 * @param subclassesId an id of subclasses
		 * @param subclasses   actual subclasses classes
		 * @param <T>          a parent of subclasses
		 */
		public <T> Builder withSubclasses(String subclassesId, List<Class<? extends T>> subclasses) {
			checkNotBuilt(this);
			//noinspection unchecked,rawtypes
			extraSubclassesMap.put(subclassesId, (List) subclasses);
			return this;
		}

		/**
		 * Sets subclasses to be serialized.
		 * Uses parent class to identify subclasses
		 * <p>
		 * <b>Order of subclasses matters. To keep serializers compatible, the order of subclasses should not change</b>
		 *
		 * @param type       a parent class  of subclasses
		 * @param subclasses actual subclasses classes
		 * @param <T>        a parent type of subclasses
		 */
		public <T> Builder withSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
			checkNotBuilt(this);
			//noinspection unchecked,rawtypes
			extraSubclassesMap.put(type, (List) subclasses);
			return this;
		}

		@Override
		protected SerializerFactory doBuild() {
			return SerializerFactory.this;
		}
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 *
	 * @see #toClassGenerator(AnnotatedType)
	 */
	public <T> BinarySerializer<T> create(DefiningClassLoader classLoader, Type type) {
		return this.<T>toClassGenerator(type).generateClassAndCreateInstance(classLoader);
	}

	public <T> BinarySerializer<T> create(Type type) {
		return create(DefiningClassLoader.create(), type);
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 *
	 * @see #toClassGenerator(AnnotatedType)
	 */
	public <T> ClassGenerator<BinarySerializer<T>> toClassGenerator(Type type) {
		return toClassGenerator(toSerializerDef(type));
	}

	public SerializerDef toSerializerDef(Type type) {
		return toSerializerDef(annotatedTypeOf(type));
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 *
	 * @see #toClassGenerator(AnnotatedType)
	 */
	public <T> BinarySerializer<T> create(DefiningClassLoader classLoader, Class<T> type) {
		return toClassGenerator(type).generateClassAndCreateInstance(classLoader);
	}

	public <T> BinarySerializer<T> create(Class<T> type) {
		return create(DefiningClassLoader.create(), type);
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 *
	 * @see #toClassGenerator(AnnotatedType)
	 */
	public <T> ClassGenerator<BinarySerializer<T>> toClassGenerator(Class<T> type) {
		return toClassGenerator(toSerializerDef(type));
	}

	public <T> SerializerDef toSerializerDef(Class<T> type) {
		return toSerializerDef(annotatedTypeOf(type));
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 *
	 * @see #toClassGenerator(AnnotatedType)
	 */
	public <T> BinarySerializer<T> create(DefiningClassLoader classLoader, TypeT<T> typeT) {
		return toClassGenerator(typeT).generateClassAndCreateInstance(classLoader);
	}

	public <T> BinarySerializer<T> create(TypeT<T> typeT) {
		return create(DefiningClassLoader.create(), typeT);
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 *
	 * @see #toClassGenerator(AnnotatedType)
	 */
	public <T> ClassGenerator<BinarySerializer<T>> toClassGenerator(TypeT<T> typeT) {
		return toClassGenerator(toSerializerDef(typeT));
	}

	public <T> SerializerDef toSerializerDef(TypeT<T> typeT) {
		return toSerializerDef(typeT.getAnnotatedType());
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 * <p>
	 *
	 * @param type a type data that would be serialized
	 * @return a generated {@link BinarySerializer}
	 */
	public <T> BinarySerializer<T> create(DefiningClassLoader classLoader, AnnotatedType type) {
		return this.<T>toClassGenerator(type).generateClassAndCreateInstance(classLoader);
	}

	public <T> BinarySerializer<T> create(AnnotatedType type) {
		return create(DefiningClassLoader.create(), type);
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerFactory}.
	 * <p>
	 *
	 * @param type a type data that would be serialized
	 * @return a generated {@link BinarySerializer}
	 */
	public <T> ClassGenerator<BinarySerializer<T>> toClassGenerator(AnnotatedType type) {
		return toClassGenerator(toSerializerDef(type));
	}

	private SerializerDef toSerializerDef(AnnotatedType type) {
		return registry.scanner(new HashMap<>()).scan(type);
	}

	/**
	 * Builds a {@link BinarySerializer} out of some {@link SerializerDef}.
	 *
	 * @param serializerDef a {@link SerializerDef} that would be used to create a {@link BinarySerializer}
	 * @return a generated {@link BinarySerializer}
	 */
	public <T> BinarySerializer<T> create(DefiningClassLoader classLoader, SerializerDef serializerDef) {
		//noinspection unchecked
		return (BinarySerializer<T>) toClassGenerator(serializerDef).generateClassAndCreateInstance(classLoader);
	}

	/**
	 * Converts a {@link SerializerDef} into a {@link ClassGenerator} of {@link BinarySerializer}
	 *
	 * @param serializer a serializer definition
	 * @param <T>        a type of data to be serialized by a {@link BinarySerializer}
	 * @return a {@link ClassGenerator} of {@link BinarySerializer}
	 */
	public <T> ClassGenerator<BinarySerializer<T>> toClassGenerator(SerializerDef serializer) {
		//noinspection unchecked
		ClassGenerator<BinarySerializer<T>>.Builder classGenerator =
			ClassGenerator.builder((Class<BinarySerializer<T>>) implementationClass, BinarySerializer.class);

		Set<Integer> collectedVersions = new HashSet<>();
		Set<SerializerDef> visited = newSetFromMap(new IdentityHashMap<>());
		Visitor visitor = new Visitor() {
			@Override
			public void visit(String serializerId, SerializerDef visitedSerializer) {
				if (!visited.add(visitedSerializer)) return;
				collectedVersions.addAll(visitedSerializer.getVersions());
				visitedSerializer.accept(this);
			}
		};
		visitor.visit(serializer);

		Integer encodeVersion = collectedVersions.stream()
			.filter(v -> v <= encodeVersionMax)
			.max(naturalOrder())
			.orElse(null);

		List<Integer> decodeVersions = collectedVersions.stream()
			.filter(v -> v >= decodeVersionMin && v <= decodeVersionMax)
			.sorted()
			.collect(toList());

		defineEncoders(classGenerator, serializer, encodeVersion);

		defineDecoders(classGenerator, serializer, decodeVersions);

		return classGenerator.build();
	}

	private void defineEncoders(ClassGenerator<?>.Builder classGenerator, SerializerDef serializer, @Nullable Integer encodeVersion) {
		StaticEncoders staticEncoders = staticEncoders(classGenerator, encodeVersion != null ? encodeVersion : 0, compatibilityLevel);

		classGenerator.withMethod("encode", int.class, List.of(byte[].class, int.class, Object.class),
			let(cast(arg(2), serializer.getEncodeType()), data ->
				encoderImpl(serializer, encodeVersion, staticEncoders, arg(0), arg(1), data)));

		classGenerator.withMethod("encode", void.class, List.of(BinaryOutput.class, Object.class),
			let(call(arg(0), "array"), buf ->
				let(call(arg(0), "pos"), pos ->
					let(cast(arg(1), serializer.getEncodeType()), data ->
						sequence(
							encoderImpl(serializer, encodeVersion, staticEncoders, buf, pos, data),
							call(arg(0), "pos", pos))))));
	}

	private Expression encoderImpl(SerializerDef serializer, @Nullable Integer encodeVersion, StaticEncoders staticEncoders, Expression buf, Variable pos, Variable data) {
		return sequence(
			encodeVersion != null ?
				writeByte(buf, pos, value((byte) (int) encodeVersion)) :
				voidExp(),

			serializer.encode(staticEncoders,
				buf, pos, data,
				encodeVersion != null ? encodeVersion : 0,
				compatibilityLevel),

			pos);
	}

	private void defineDecoders(
		ClassGenerator<?>.Builder classGenerator, SerializerDef serializer, List<Integer> decodeVersions
	) {
		Integer latestVersion = decodeVersions.isEmpty() ? null : decodeVersions.get(decodeVersions.size() - 1);
		StaticDecoders latestStaticDecoders = staticDecoders(classGenerator, latestVersion == null ? 0 : latestVersion);
		classGenerator.withMethod("decode", Object.class, List.of(BinaryInput.class),
			decodeImpl(serializer, latestVersion, latestStaticDecoders, arg(0)));

		classGenerator.withMethod("decode", Object.class, List.of(byte[].class, int.class),
			let(constructor(BinaryInput.class, arg(0), arg(1)), in ->
				decodeImpl(serializer, latestVersion, latestStaticDecoders, in)));

		classGenerator.withMethod("decodeEarlierVersions",
			serializer.getDecodeType(),
			List.of(BinaryInput.class, byte.class),
			() -> {
				List<Expression> listKey = new ArrayList<>();
				List<Expression> listValue = new ArrayList<>();
				for (int i = decodeVersions.size() - 2; i >= 0; i--) {
					int version = decodeVersions.get(i);
					listKey.add(value((byte) version));
					listValue.add(call(self(), "decodeVersion" + version, arg(0)));
				}
				Expression result = throwException(CorruptedDataException.class,
					concat(value("Unsupported version: "), arg(1), value(", supported versions: " + decodeVersions)));
				for (int i = listKey.size() - 1; i >= 0; i--) {
					result = ifEq(arg(1), listKey.get(i), listValue.get(i), result);
				}
				return result;
			});

		for (int i = decodeVersions.size() - 2; i >= 0; i--) {
			int version = decodeVersions.get(i);
			classGenerator.withMethod("decodeVersion" + version, serializer.getDecodeType(), List.of(BinaryInput.class),
				sequence(serializer
					.defineDecoder(staticDecoders(classGenerator, version), version, compatibilityLevel)
					.decode(arg(0))));
		}
	}

	private Expression decodeImpl(
		SerializerDef serializer, Integer latestVersion, StaticDecoders staticDecoders, Expression in
	) {
		return latestVersion == null ?
			serializer.decode(
				staticDecoders,
				in,
				0,
				compatibilityLevel) :

			let(readByte(in),
				version -> ifEq(version, value((byte) (int) latestVersion),
					serializer.decode(
						staticDecoders,
						in,
						latestVersion,
						compatibilityLevel),
					call(self(), "decodeEarlierVersions", in, version)));
	}

	private static StaticEncoders staticEncoders(ClassGenerator<?>.Builder classGenerator, int version, CompatibilityLevel compatibilityLevel) {
		return new StaticEncoders() {
			final Map<List<?>, String> defined = new HashMap<>();

			@Override
			public Encoder define(SerializerDef serializerDef) {
				List<?> key = List.of(identityHashCode(serializerDef), version, compatibilityLevel);
				String methodName = defined.get(key);
				if (methodName == null) {
					for (int i = 1; ; i++) {
						methodName =
							"encode_" +
							serializerDef.getEncodeType().getSimpleName()
								.replace('[', 's')
								.replace(']', '_') +
							(i == 1 ? "" : "_" + i);
						if (defined.values().stream().noneMatch(methodName::equals)) break;
					}
					defined.put(key, methodName);
					classGenerator.withStaticMethod(methodName, int.class, List.of(byte[].class, int.class, serializerDef.getEncodeType()), sequence(
						serializerDef.encode(this, BUF, POS, VALUE, version, compatibilityLevel),
						POS));
				}
				String finalMethodName = methodName;
				return (buf, pos, value) -> Expressions.set(pos, staticCallSelf(finalMethodName, buf, pos, value));
			}
		};
	}

	private StaticDecoders staticDecoders(ClassGenerator<?>.Builder classGenerator, int version) {
		return new StaticDecoders() {
			final Map<List<?>, String> defined = new HashMap<>();

			@Override
			public Decoder define(SerializerDef serializerDef) {
				List<?> key = List.of(identityHashCode(serializerDef), version, compatibilityLevel);
				String methodName = defined.get(key);
				if (methodName == null) {
					for (int i = 1; ; i++) {
						methodName =
							"decode_" +
							serializerDef.getDecodeType().getSimpleName()
								.replace('[', 's')
								.replace(']', '_') +
							("_V" + version) +
							(i == 1 ? "" : "_" + i);
						if (defined.values().stream().noneMatch(methodName::equals)) break;
					}
					defined.put(key, methodName);
					classGenerator.withStaticMethod(methodName, serializerDef.getDecodeType(), List.of(BinaryInput.class),
						serializerDef.decode(this, IN, version, compatibilityLevel));
				}
				String finalMethodName = methodName;
				return in -> staticCallSelf(finalMethodName, in);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private SerializerDef scan(Context<SerializerDef> ctx) {
		Map<Type, SerializerDef> cache = (Map<Type, SerializerDef>) ctx.getContextValue();
		SerializerDef serializerDef = cache.get(ctx.getType());
		if (serializerDef != null) return serializerDef;
		ForwardingSerializerDefImpl forwardingSerializerDef = new ForwardingSerializerDefImpl();
		cache.put(ctx.getType(), forwardingSerializerDef);

		SerializerDef serializer = doScan(ctx);

		forwardingSerializerDef.serializerDef = serializer;

		return serializer;
	}

	private SerializerDef doScan(Context<SerializerDef> ctx) {
		Class<?> rawClass = ctx.getRawType();
		if (rawClass.isAnonymousClass())
			throw new IllegalArgumentException("Class " + rawClass.getName() + " should not be anonymous");
		if (rawClass.isLocalClass())
			throw new IllegalArgumentException("Class " + rawClass.getName() + " should not be local");
		if (rawClass.getEnclosingClass() != null && !Modifier.isStatic(rawClass.getModifiers()))
			throw new IllegalArgumentException("Class " + rawClass.getName() + "should not be an inner class");

		ClassSerializerDef.Builder classSerializerBuilder = ClassSerializerDef.builder(rawClass);
		if (rawClass.getAnnotation(SerializeRecord.class) != null) {
			if (!rawClass.isRecord()) {
				throw new IllegalArgumentException(
					"Non-record type '" + rawClass.getName() +
					"' annotated with @SerializeRecord annotation");
			}
			scanRecord(ctx, classSerializerBuilder);
		} else {
			scanStaticFactoryMethods(ctx, classSerializerBuilder);
			if (!Modifier.isAbstract(rawClass.getModifiers())) {
				scanConstructors(ctx, classSerializerBuilder);
			}
			scanClass(ctx, classSerializerBuilder);
			classSerializerBuilder.withMatchingSetters();
		}
		return classSerializerBuilder.build();
	}

	private void scanClass(Context<SerializerDef> ctx, ClassSerializerDef.Builder classSerializerBuilder) {
		AnnotatedType annotatedClassType = ctx.getAnnotatedType();

		Class<?> rawClassType = getRawType(annotatedClassType);
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(annotatedClassType)::get;

		if (rawClassType.getSuperclass() != Object.class) {
			scanClass(ctx.push(bind(rawClassType.getAnnotatedSuperclass(), bindings)), classSerializerBuilder);
		}

		List<MemberSerializer> memberSerializers = new ArrayList<>();
		scanFields(ctx, bindings, memberSerializers);
		scanGetters(ctx, bindings, memberSerializers);
		scanSetters(ctx, classSerializerBuilder);
		resolveMembersOrder(ctx.getRawType(), memberSerializers);
		addMemberSerializersToSerializerBuilder(classSerializerBuilder, memberSerializers);
	}

	private void scanRecord(Context<SerializerDef> ctx, ClassSerializerDef.Builder classSerializerBuilder) {
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(ctx.getAnnotatedType())::get;
		List<MemberSerializer> memberSerializers = new ArrayList<>();

		Class<?> rawType = ctx.getRawType();
		int order = 1;
		for (RecordComponent recordComponent : rawType.getRecordComponents()) {
			String name = recordComponent.getName();

			Method method;
			try {
				method = rawType.getMethod(name);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException(e);
			}
			MemberSerializer memberSerializer = new MemberSerializer(method, order++, Serialize.DEFAULT_VERSION, Serialize.DEFAULT_VERSION);
			memberSerializer.serializer = ctx.scan(bind(recordComponent.getAnnotatedType(), bindings));
			memberSerializers.add(memberSerializer);
		}
		classSerializerBuilder.withConstructor(rawType.getConstructors()[0], Arrays.stream(rawType.getRecordComponents()).map(RecordComponent::getName).toList());
		resolveMembersOrder(ctx.getRawType(), memberSerializers);
		addMemberSerializersToSerializerBuilder(classSerializerBuilder, memberSerializers);
	}

	private void scanFields(
		Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
		List<MemberSerializer> memberSerializers
	) {
		for (Field field : ctx.getRawType().getDeclaredFields()) {
			@Nullable SerializerFactory.MemberSerializer memberSerializer = findAnnotations(field, field.getAnnotations());
			if (memberSerializer == null) continue;

			if (!isPublic(field.getModifiers()))
				throw new IllegalArgumentException(format("Field %s must be public", field));
			if (isStatic(field.getModifiers()))
				throw new IllegalArgumentException(format("Field %s must not be static", field));
			if (isTransient(field.getModifiers()))
				throw new IllegalArgumentException(format("Field %s must not be transient", field));

			memberSerializer.serializer = ctx.scan(bind(field.getAnnotatedType(), bindings));
			memberSerializers.add(memberSerializer);
		}
	}

	private void scanGetters(
		Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
		List<MemberSerializer> memberSerializers
	) {
		for (Method method : ctx.getRawType().getDeclaredMethods()) {
			if (method.isBridge()) continue;

			@Nullable SerializerFactory.MemberSerializer memberSerializer = findAnnotations(method, method.getAnnotations());
			if (memberSerializer == null) continue;

			if (!isPublic(method.getModifiers()))
				throw new IllegalArgumentException(format("Getter %s must be public", method));
			if (isStatic(method.getModifiers()))
				throw new IllegalArgumentException(format("Getter %s must not be static", method));
			if (method.getReturnType() == Void.TYPE || method.getParameterTypes().length != 0)
				throw new IllegalArgumentException(format("%s must be getter", method));

			memberSerializer.serializer = ctx.scan(bind(method.getAnnotatedReturnType(), bindings));
			memberSerializers.add(memberSerializer);
		}
	}

	private void scanSetters(Context<SerializerDef> ctx, ClassSerializerDef.Builder classSerializerBuilder) {
		for (Method method : ctx.getRawType().getDeclaredMethods()) {
			if (isStatic(method.getModifiers())) continue;
			if (method.getParameterTypes().length != 0) {
				List<String> fields = extractFields(method);
				if (fields.size() == method.getParameterTypes().length) {
					classSerializerBuilder.withSetter(method, fields);
				} else {
					if (!fields.isEmpty())
						throw new IllegalArgumentException("Fields should not be empty");
				}
			}
		}
	}

	private void scanStaticFactoryMethods(Context<SerializerDef> ctx, ClassSerializerDef.Builder classSerializerBuilder) {
		Class<?> factoryClassType = ctx.getRawType();
		for (Method factory : factoryClassType.getDeclaredMethods()) {
			if (ctx.getRawType() != factory.getReturnType()) {
				continue;
			}
			if (factory.getParameterTypes().length != 0) {
				List<String> fields = extractFields(factory);
				if (fields.size() == factory.getParameterTypes().length) {
					classSerializerBuilder.withStaticFactoryMethod(factory, fields);
				} else {
					if (!fields.isEmpty())
						throw new IllegalArgumentException(format("@Deserialize is not fully specified for %s", fields));
				}
			}
		}
	}

	private List<String> extractFields(Method method) {
		List<String> fields = new ArrayList<>(method.getParameterTypes().length);
		for (int i = 0; i < method.getParameterTypes().length; i++) {
			Annotation[] parameterAnnotations = method.getParameterAnnotations()[i];
			Deserialize annotation = getAnnotation(parameterAnnotations, Deserialize.class);
			if (annotation != null) {
				String field = annotation.value();
				fields.add(field);
			}
		}
		return fields;
	}

	private void scanConstructors(Context<SerializerDef> ctx, ClassSerializerDef.Builder classSerializerBuilder) {
		boolean found = false;
		for (Constructor<?> constructor : ctx.getRawType().getDeclaredConstructors()) {
			List<String> fields = new ArrayList<>(constructor.getParameterTypes().length);
			for (int i = 0; i < constructor.getParameterTypes().length; i++) {
				Deserialize annotation = getAnnotation(constructor.getParameterAnnotations()[i], Deserialize.class);
				if (annotation != null) {
					String field = annotation.value();
					fields.add(field);
				}
			}
			if (constructor.getParameterTypes().length != 0 &&
				fields.size() == constructor.getParameterTypes().length
			) {
				if (found)
					throw new IllegalArgumentException(format("Duplicate @Deserialize constructor %s", constructor));
				found = true;
				classSerializerBuilder.withConstructor(constructor, fields);
			} else {
				if (!fields.isEmpty())
					throw new IllegalArgumentException(format("@Deserialize is not fully specified for %s", fields));
			}
		}
	}

	private static void addMemberSerializersToSerializerBuilder(ClassSerializerDef.Builder classSerializerBuilder, List<MemberSerializer> memberSerializers) {
		Set<Integer> orders = new HashSet<>();
		for (MemberSerializer memberSerializer : memberSerializers) {
			if (!orders.add(memberSerializer.order))
				throw new IllegalArgumentException(format("Duplicate order %s for %s", memberSerializer.order, memberSerializer));
		}

		Collections.sort(memberSerializers);
		for (MemberSerializer memberSerializer : memberSerializers) {
			if (memberSerializer.member instanceof Method) {
				classSerializerBuilder.withGetter((Method) memberSerializer.member, memberSerializer.serializer, memberSerializer.added, memberSerializer.removed);
			} else {
				classSerializerBuilder.withField((Field) memberSerializer.member, memberSerializer.serializer, memberSerializer.added, memberSerializer.removed);
			}
		}
	}

	private static void resolveMembersOrder(Class<?> clazz, List<MemberSerializer> memberSerializers) {
		if (memberSerializers.stream().noneMatch(f -> f.order == Integer.MIN_VALUE)) {
			return;
		}
		if (memberSerializers.stream().anyMatch(f -> f.order != Integer.MIN_VALUE)) {
			throw new IllegalArgumentException("Found mixed explicit and auto-ordering properties in " + clazz);
		}
		Map<String, List<MemberSerializer>> membersMap = memberSerializers.stream().collect(groupingBy(MemberSerializer::getName, toList()));
		String pathToClass = clazz.getName().replace('.', '/') + ".class";
		try (InputStream classInputStream = clazz.getClassLoader().getResourceAsStream(pathToClass)) {
			ClassReader cr = new ClassReader(requireNonNull(classInputStream));
			cr.accept(new ClassVisitor(Opcodes.ASM8) {
				int index = 0;

				@Override
				public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
					List<MemberSerializer> list = membersMap.get(name);
					if (list == null) return null;
					for (MemberSerializer memberSerializer : list) {
						if (!(memberSerializer.member instanceof Field)) continue;
						memberSerializer.order = index++;
						break;
					}
					return null;
				}

				@Override
				public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
					List<MemberSerializer> list = membersMap.get(name);
					if (list == null) return null;
					for (MemberSerializer memberSerializer : list) {
						if (!(memberSerializer.member instanceof Method)) continue;
						if (!descriptor.equals(getType((Method) memberSerializer.member).getDescriptor()))
							continue;
						memberSerializer.order = index++;
						break;
					}
					return null;
				}
			}, SKIP_CODE | SKIP_DEBUG | SKIP_FRAMES);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private @Nullable SerializerFactory.MemberSerializer findAnnotations(Member member, Annotation[] annotations) {
		int added = Serialize.DEFAULT_VERSION;
		int removed = Serialize.DEFAULT_VERSION;

		Serialize serialize = getAnnotation(annotations, Serialize.class);
		if (serialize != null) {
			added = serialize.added();
			removed = serialize.removed();
		}

		SerializeProfiles profiles = getAnnotation(annotations, SerializeProfiles.class);
		if (profiles != null) {
			if (!List.of(profiles.value()).contains(profile == null ? "" : profile)) {
				return null;
			}
			int addedProfile = getProfileVersion(profiles.value(), profiles.added());
			if (addedProfile != SerializeProfiles.DEFAULT_VERSION) {
				added = addedProfile;
			}
			int removedProfile = getProfileVersion(profiles.value(), profiles.removed());
			if (removedProfile != SerializeProfiles.DEFAULT_VERSION) {
				removed = removedProfile;
			}
		}

		return serialize != null ? new MemberSerializer(member, serialize.order(), added, removed) : null;
	}

	private int getProfileVersion(String[] profiles, int[] versions) {
		if (profiles == null || profiles.length == 0) {
			return SerializeProfiles.DEFAULT_VERSION;
		}
		for (int i = 0; i < profiles.length; i++) {
			if (Objects.equals(profile, profiles[i])) {
				if (i < versions.length) {
					return versions[i];
				}
				return SerializeProfiles.DEFAULT_VERSION;
			}
		}
		return SerializeProfiles.DEFAULT_VERSION;
	}

	public static final class MemberSerializer implements Comparable<MemberSerializer> {
		final Member member;
		int order;
		final int added;
		final int removed;
		//		final TypedModsMap mods;
		SerializerDef serializer;

		private MemberSerializer(Member member, int order, int added, int removed) {
			this.member = member;
			this.order = order;
			this.added = added;
			this.removed = removed;
		}

		public String getName() {
			return member.getName();
		}

		private int fieldRank() {
			return member instanceof Method ? 2 : 1;
		}

		@Override
		public int compareTo(MemberSerializer o) {
			int result = Integer.compare(this.order, o.order);
			if (result != 0) {
				return result;
			}
			result = Integer.compare(fieldRank(), o.fieldRank());
			if (result != 0) {
				return result;
			}
			result = getName().compareTo(o.getName());
			return result;
		}

		@Override
		public String toString() {
			return member.getClass().getSimpleName() + " " + getName();
		}
	}

	public static class ForwardingSerializerDefImpl extends ForwardingSerializerDef {
		SerializerDef serializerDef;

		@Override
		protected SerializerDef serializer() {
			return serializerDef;
		}
	}

}
