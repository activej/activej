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

import io.activej.codegen.BytecodeStorage;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.codegen.util.WithInitializer;
import io.activej.serializer.annotations.*;
import io.activej.serializer.impl.*;
import io.activej.types.AnnotationUtils;
import io.activej.types.TypeT;
import io.activej.types.scanner.TypeScannerRegistry;
import io.activej.types.scanner.TypeScannerRegistry.Context;
import io.activej.types.scanner.TypeScannerRegistry.Mapping;
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
import static io.activej.serializer.SerializerDef.*;
import static io.activej.serializer.impl.SerializerExpressions.readByte;
import static io.activej.serializer.impl.SerializerExpressions.writeByte;
import static io.activej.serializer.util.Utils.get;
import static io.activej.types.AnnotatedTypes.*;
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
public final class SerializerBuilder implements WithInitializer<SerializerBuilder> {
	private final DefiningClassLoader classLoader;

	private final TypeScannerRegistry<SerializerDef> registry = TypeScannerRegistry.create();

	private Class<?> implementationClass = Object.class;

	private String profile;
	private int encodeVersionMax = Integer.MAX_VALUE;
	private int decodeVersionMin = 0;
	private int decodeVersionMax = Integer.MAX_VALUE;
	private CompatibilityLevel compatibilityLevel = CompatibilityLevel.LEVEL_4;
	private int autoOrderingStart = 1;
	private int autoOrderingStride = 1;
	private boolean annotationsCompatibilityMode;

	private final Map<Object, List<Class<?>>> extraSubclassesMap = new HashMap<>();

	private final Map<Class<? extends Annotation>, Map<Class<? extends Annotation>, Function<? extends Annotation, ? extends Annotation>>> annotationAliases = new HashMap<>();

	private SerializerBuilder(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Creates a new instance of {@code SerializerBuilder} with newly created {@link DefiningClassLoader}
	 */
	public static SerializerBuilder create() {
		return create(DefiningClassLoader.create());
	}

	/**
	 * Creates a new instance of {@code SerializerBuilder} with external {@link DefiningClassLoader}
	 */
	public static SerializerBuilder create(DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = new SerializerBuilder(definingClassLoader);

		builder
				.with(boolean.class, ctx -> new SerializerDefBoolean(false))
				.with(char.class, ctx -> new SerializerDefChar(false))
				.with(byte.class, ctx -> new SerializerDefByte(false))
				.with(short.class, ctx -> new SerializerDefShort(false))
				.with(int.class, ctx -> new SerializerDefInt(false))
				.with(long.class, ctx -> new SerializerDefLong(false))
				.with(float.class, ctx -> new SerializerDefFloat(false))
				.with(double.class, ctx -> new SerializerDefDouble(false))

				.with(Boolean.class, ctx -> new SerializerDefBoolean(true))
				.with(Character.class, ctx -> new SerializerDefChar(true))
				.with(Byte.class, ctx -> new SerializerDefByte(true))
				.with(Short.class, ctx -> new SerializerDefShort(true))
				.with(Integer.class, ctx -> new SerializerDefInt(true))
				.with(Long.class, ctx -> new SerializerDefLong(true))
				.with(Float.class, ctx -> new SerializerDefFloat(true))
				.with(Double.class, ctx -> new SerializerDefDouble(true));

		for (Type type : new Type[]{
				boolean[].class, char[].class, byte[].class, short[].class, int[].class, long[].class, float[].class, double[].class,
				Object[].class}) {
			builder.with(type, ctx -> new SerializerDefArray(ctx.scanTypeArgument(0), ctx.getRawType()));
		}

		LinkedHashMap<Class<?>, SerializerDef> addressMap = new LinkedHashMap<>();
		addressMap.put(Inet4Address.class, new SerializerDefInet4Address());
		addressMap.put(Inet6Address.class, new SerializerDefInet6Address());
		builder
				.with(Inet4Address.class, ctx -> new SerializerDefInet4Address())
				.with(Inet6Address.class, ctx -> new SerializerDefInet6Address())
				.with(InetAddress.class, ctx -> new SerializerDefSubclass(InetAddress.class, addressMap, 0));

		builder
				.with(Enum.class, ctx -> {
					for (Method method : ctx.getRawType().getDeclaredMethods()) {
						if (AnnotationUtils.hasAnnotation(Serialize.class, method.getAnnotations())) {
							return builder.scan(ctx);
						}
					}
					for (Field field : ctx.getRawType().getDeclaredFields()) {
						if (AnnotationUtils.hasAnnotation(Serialize.class, field.getAnnotations())) {
							return builder.scan(ctx);
						}
					}
					//noinspection unchecked
					return new SerializerDefEnum((Class<? extends Enum<?>>) ctx.getRawType());
				})

				.with(String.class, ctx -> {
					SerializeStringFormat a = builder.getAnnotation(SerializeStringFormat.class, ctx.getAnnotations());
					return new SerializerDefString(a == null ? StringFormat.UTF8 : a.value());
				})

				.with(Collection.class, ctx -> new SerializerDefRegularCollection(ctx.scanTypeArgument(0), Collection.class, ArrayList.class))
				.with(Queue.class, ctx -> new SerializerDefRegularCollection(ctx.scanTypeArgument(0), Queue.class, ArrayDeque.class))

				.with(List.class, ctx -> new SerializerDefList(ctx.scanTypeArgument(0)))
				.with(ArrayList.class, ctx -> new SerializerDefRegularCollection(ctx.scanTypeArgument(0), ArrayList.class, ArrayList.class))
				.with(LinkedList.class, ctx -> new SerializerDefLinkedList(ctx.scanTypeArgument(0)))

				.with(Map.class, ctx -> new SerializerDefMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1)))
				.with(HashMap.class, ctx -> new SerializerDefHashMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1), HashMap.class, HashMap.class))
				.with(LinkedHashMap.class, ctx -> new SerializerDefHashMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1), LinkedHashMap.class, LinkedHashMap.class))
				.with(EnumMap.class, ctx -> new SerializerDefEnumMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1)))

				.with(Set.class, ctx -> new SerializerDefSet(ctx.scanTypeArgument(0)))
				.with(HashSet.class, ctx -> new SerializerDefHashSet(ctx.scanTypeArgument(0), HashSet.class, HashSet.class))
				.with(LinkedHashSet.class, ctx -> new SerializerDefHashSet(ctx.scanTypeArgument(0), LinkedHashSet.class, LinkedHashSet.class))
				.with(EnumSet.class, ctx -> new SerializerDefEnumSet(ctx.scanTypeArgument(0)))

				.with(Object.class, builder::scan);

		return builder;
	}

	/**
	 * Adds a mapping to resolve a {@link SerializerDef} for a given {@link TypeT}
	 *
	 * @param typeT a type token
	 * @param fn    a mapping to resolve a serializer
	 */
	public SerializerBuilder with(TypeT<?> typeT, Mapping<SerializerDef> fn) {
		return with(typeT.getType(), fn);
	}

	/**
	 * Adds a mapping to resolve a {@link SerializerDef} for a given {@link Type}
	 *
	 * @param type a type
	 * @param fn   a mapping to resolve a serializer
	 */
	@SuppressWarnings("PointlessBooleanExpression")
	public SerializerBuilder with(Type type, Mapping<SerializerDef> fn) {
		registry.with(type, ctx -> {
			Class<?> rawClass = ctx.getRawType();
			SerializerDef serializerDef;
			SerializeClass annotationClass;
			if (false ||
					(annotationClass = getAnnotation(SerializeClass.class, ctx.getAnnotations())) != null ||
					(annotationClass = getAnnotation(SerializeClass.class, rawClass.getAnnotations())) != null) {
				if (annotationClass.value() != SerializerDef.class) {
					try {
						serializerDef = annotationClass.value().getDeclaredConstructor().newInstance();
					} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
						throw new RuntimeException(e);
					}
				} else {
					LinkedHashMap<Class<?>, SerializerDef> map = new LinkedHashMap<>();
					LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(List.of(annotationClass.subclasses()));
					subclassesSet.addAll(extraSubclassesMap.getOrDefault(rawClass, List.of()));
					subclassesSet.addAll(extraSubclassesMap.getOrDefault(annotationClass.subclassesId(), List.of()));
					for (Class<?> subclass : subclassesSet) {
						map.put(subclass, ctx.scan(subclass));
					}
					serializerDef = new SerializerDefSubclass(rawClass, map, annotationClass.subclassesIdx());
				}
			} else if (extraSubclassesMap.containsKey(rawClass)) {
				LinkedHashMap<Class<?>, SerializerDef> map = new LinkedHashMap<>();
				for (Class<?> subclass : extraSubclassesMap.get(rawClass)) {
					map.put(subclass, ctx.scan(subclass));
				}
				serializerDef = new SerializerDefSubclass(rawClass, map, 0);
			} else {
				serializerDef = fn.apply(ctx);
			}

			if (hasAnnotation(SerializeReference.class, ctx.getAnnotations()) ||
					hasAnnotation(SerializeReference.class, rawClass.getAnnotations())) {
				serializerDef = new SerializerDefReference(serializerDef);
			}

			if (hasAnnotation(SerializeVarLength.class, ctx.getAnnotations())) {
				serializerDef = ((SerializerDefWithVarLength) serializerDef).ensureVarLength();
			}

			SerializeFixedSize annotationFixedSize;
			if ((annotationFixedSize = getAnnotation(SerializeFixedSize.class, ctx.getAnnotations())) != null) {
				serializerDef = ((SerializerDefWithFixedSize) serializerDef).ensureFixedSize(annotationFixedSize.value());
			}

			if (hasAnnotation(SerializeNullable.class, ctx.getAnnotations())) {
				serializerDef = serializerDef instanceof SerializerDefWithNullable ?
						((SerializerDefWithNullable) serializerDef).ensureNullable(compatibilityLevel) : new SerializerDefNullable(serializerDef);
			}

			return serializerDef;
		});
		return this;
	}

	@SuppressWarnings({"unchecked", "ForLoopReplaceableByForEach"})
	private <A extends Annotation> @Nullable A getAnnotation(Class<A> type, Annotation[] annotations) {
		for (int i = 0; i < annotations.length; i++) {
			Annotation annotation = annotations[i];
			if (annotation.annotationType() == type) {
				return (A) annotation;
			}
		}
		Map<Class<? extends Annotation>, Function<? extends Annotation, ? extends Annotation>> aliasesMap = annotationAliases.get(type);
		if (aliasesMap != null) {
			for (int i = 0; i < annotations.length; i++) {
				Annotation annotation = annotations[i];
				Function<Annotation, ? extends Annotation> mapping = (Function<Annotation, ? extends Annotation>) aliasesMap.get(annotation.annotationType());
				if (mapping != null) {
					return (A) mapping.apply(annotation);
				}
			}
		}
		return null;
	}

	@SuppressWarnings("ForLoopReplaceableByForEach")
	private <A extends Annotation> boolean hasAnnotation(Class<A> type, Annotation[] annotations) {
		Map<Class<? extends Annotation>, Function<? extends Annotation, ? extends Annotation>> aliasesMap = annotationAliases.getOrDefault(type, Map.of());
		for (int i = 0; i < annotations.length; i++) {
			Class<? extends Annotation> annotationType = annotations[i].annotationType();
			if (annotationType == type || aliasesMap.containsKey(annotationType)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Adds an implementation class for the serializer
	 *
	 * @param implementationClass an implementation class
	 */
	public SerializerBuilder withImplementationClass(Class<?> implementationClass) {
		this.implementationClass = implementationClass;
		return this;
	}

	/**
	 * Sets a given {@link CompatibilityLevel} for the serializer. This method should be used
	 * to ensure backwards compatibility with previous versions of serializers
	 *
	 * @param compatibilityLevel a compatibility level
	 */
	public SerializerBuilder withCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
		this.compatibilityLevel = compatibilityLevel;
		return this;
	}

	/**
	 * Enables annotation compatibility mode
	 *
	 * @see #withAnnotationCompatibilityMode(boolean)
	 */
	public SerializerBuilder withAnnotationCompatibilityMode() {
		return withAnnotationCompatibilityMode(true);
	}

	/**
	 * Enables or disables annotation compatibility mode
	 * <p>
	 * In previous ActiveJ versions serializer annotations had to be placed directly on fields/getters.
	 * To specify a concrete annotated type a {@code path} attribute was used. Now it is possible to
	 * annotate types directly. However, for compatibility with classes annotated using a {@code path} attribute
	 * this compatibility mode can be enabled
	 */
	public SerializerBuilder withAnnotationCompatibilityMode(boolean annotationsCompatibilityMode) {
		this.annotationsCompatibilityMode = annotationsCompatibilityMode;
		return this;
	}

	/**
	 * Adds alias annotation for a serializer annotation. Alias annotation acts as if it is a regular
	 * serializer annotation
	 *
	 * @param annotation      a serializer annotation
	 * @param annotationAlias an alias annotation
	 * @param mapping         a function that transforms an alias annotation into a serializer annotation
	 * @param <A>             a type of serializer annotation
	 * @param <T>             a type of alias annotation
	 */
	public <A extends Annotation, T extends Annotation> SerializerBuilder withAnnotationAlias(Class<A> annotation, Class<T> annotationAlias,
			Function<T, A> mapping) {
		annotationAliases.computeIfAbsent(annotation, $ -> new HashMap<>()).put(annotationAlias, mapping);
		return this;
	}

	/**
	 * Sets maximal encode version
	 * <p>
	 * This method is used to ensure compatibility between different versions of serialized objects
	 *
	 * @param encodeVersionMax a maximal encode version
	 */
	public SerializerBuilder withEncodeVersion(int encodeVersionMax) {
		this.encodeVersionMax = encodeVersionMax;
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
	public SerializerBuilder withDecodeVersions(int decodeVersionMin, int decodeVersionMax) {
		this.decodeVersionMin = decodeVersionMin;
		this.decodeVersionMax = decodeVersionMax;
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
	public SerializerBuilder withVersions(int encodeVersionMax, int decodeVersionMin, int decodeVersionMax) {
		this.encodeVersionMax = encodeVersionMax;
		this.decodeVersionMin = decodeVersionMin;
		this.decodeVersionMax = decodeVersionMax;
		return this;
	}

	/**
	 * Sets auto ordering parameters (used when no explicit ordering is set)
	 *
	 * @param autoOrderingStart  a value of initial order index
	 * @param autoOrderingStride a step between indices
	 */
	public SerializerBuilder withAutoOrdering(int autoOrderingStart, int autoOrderingStride) {
		this.autoOrderingStart = autoOrderingStart;
		this.autoOrderingStride = autoOrderingStride;
		return this;
	}

	/**
	 * Sets a serializer profile
	 *
	 * @param profile a serializer profile
	 */
	public SerializerBuilder withProfile(String profile) {
		this.profile = profile;
		return this;
	}

	/**
	 * Sets subclasses to be serialized.
	 * Uses custom string id to identify subclasses
	 *
	 * @param subclassesId an id of subclasses
	 * @param subclasses   actual subclasses classes
	 * @param <T>          a parent of subclasses
	 */
	public <T> SerializerBuilder withSubclasses(String subclassesId, List<Class<? extends T>> subclasses) {
		//noinspection unchecked,rawtypes
		extraSubclassesMap.put(subclassesId, (List) subclasses);
		return this;
	}

	/**
	 * Sets subclasses to be serialized.
	 * Uses parent class to identify subclasses
	 *
	 * @param type       a parent class  of subclasses
	 * @param subclasses actual subclasses classes
	 * @param <T>        a parent type of subclasses
	 */
	public <T> SerializerBuilder withSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
		//noinspection unchecked,rawtypes
		extraSubclassesMap.put(type, (List) subclasses);
		return this;
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 *
	 * @see #build(AnnotatedType)
	 */
	public <T> BinarySerializer<T> build(Type type) {
		return build(annotatedTypeOf(type));
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 *
	 * @see #build(AnnotatedType)
	 */
	public <T> BinarySerializer<T> build(Class<T> type) {
		return build(annotatedTypeOf(type));
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 *
	 * @see #build(AnnotatedType)
	 */
	public <T> BinarySerializer<T> build(TypeT<T> typeT) {
		return build(typeT.getAnnotatedType());
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 * <p>
	 *
	 * @param type a type data that would be serialized
	 * @return a generated {@link BinarySerializer}
	 */
	public <T> BinarySerializer<T> build(AnnotatedType type) {
		SerializerDef serializer = registry.scanner(new HashMap<>()).scan(type);
		ClassBuilder<BinarySerializer<T>> classBuilder = toClassBuilder(serializer);
		return classBuilder.defineClassAndCreateInstance(classLoader);
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 *
	 * @see #build(String, AnnotatedType)
	 */
	public <T> BinarySerializer<T> build(String className, Type type) {
		return build(className, annotatedTypeOf(type));
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 *
	 * @see #build(String, AnnotatedType)
	 */
	public <T> BinarySerializer<T> build(String className, Class<T> type) {
		return build(className, annotatedTypeOf(type));
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 *
	 * @see #build(String, AnnotatedType)
	 */
	public <T> BinarySerializer<T> build(String className, TypeT<T> typeT) {
		return build(className, typeT.getAnnotatedType());
	}

	/**
	 * Builds a {@link BinarySerializer} out of {@code this} {@link SerializerBuilder}.
	 * <p>
	 * A built serializer would have a class name equal to the one that passed to this method.
	 * <p>
	 * If {@link #classLoader} has already defined the serializer class, the class would be taken from
	 * the class loader's cache.
	 * Moreover, if a {@link DefiningClassLoader} has a persistent {@link BytecodeStorage},
	 * the serializer class would be taken from that persistent cache.
	 *
	 * @param className a name of the class of a serializer
	 * @param type      a type data that would be serialized
	 * @return a generated {@link BinarySerializer}
	 */
	public <T> BinarySerializer<T> build(String className, AnnotatedType type) {
		return classLoader.ensureClassAndCreateInstance(
				className,
				() -> {
					SerializerDef serializer = registry.scanner(new HashMap<>()).scan(type);
					return toClassBuilder(serializer);
				});
	}

	/**
	 * Builds a {@link BinarySerializer} out of some {@link SerializerDef}.
	 *
	 * @param serializer a {@link SerializerDef} that would be used to create a {@link BinarySerializer}
	 * @return a generated {@link BinarySerializer}
	 */
	public <T> BinarySerializer<T> build(SerializerDef serializer) {
		//noinspection unchecked
		return (BinarySerializer<T>) toClassBuilder(serializer).defineClassAndCreateInstance(DefiningClassLoader.create());
	}

	/**
	 * Converts a {@link SerializerDef} into a {@link ClassBuilder} of {@link BinarySerializer}
	 *
	 * @param serializer a serializer definition
	 * @param <T>        a type of data to be serialized by a {@link BinarySerializer}
	 * @return a {@link ClassBuilder} of {@link BinarySerializer}
	 */
	public <T> ClassBuilder<BinarySerializer<T>> toClassBuilder(SerializerDef serializer) {
		//noinspection unchecked
		ClassBuilder<BinarySerializer<T>> classBuilder = ClassBuilder.create((Class<BinarySerializer<T>>) implementationClass, BinarySerializer.class);

		Set<Integer> collectedVersions = new HashSet<>();
		Map<Object, Expression> encoderInitializers = new HashMap<>();
		Map<Object, Expression> decoderInitializers = new HashMap<>();
		Map<Object, Expression> encoderFinalizers = new HashMap<>();
		Map<Object, Expression> decoderFinalizers = new HashMap<>();
		Set<SerializerDef> visited = newSetFromMap(new IdentityHashMap<>());
		Visitor visitor = new Visitor() {
			@Override
			public void visit(String serializerId, SerializerDef visitedSerializer) {
				if (!visited.add(visitedSerializer)) return;
				collectedVersions.addAll(visitedSerializer.getVersions());
				encoderInitializers.putAll(visitedSerializer.getEncoderInitializer());
				decoderInitializers.putAll(visitedSerializer.getDecoderInitializer());
				encoderFinalizers.putAll(visitedSerializer.getEncoderFinalizer());
				decoderFinalizers.putAll(visitedSerializer.getDecoderFinalizer());
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

		defineEncoders(classBuilder, serializer, encodeVersion,
				new ArrayList<>(encoderInitializers.values()), new ArrayList<>(encoderFinalizers.values()));

		defineDecoders(classBuilder, serializer, decodeVersions,
				new ArrayList<>(decoderInitializers.values()), new ArrayList<>(decoderFinalizers.values()));

		return classBuilder;
	}

	private void defineEncoders(ClassBuilder<?> classBuilder, SerializerDef serializer, @Nullable Integer encodeVersion,
			List<Expression> encoderInitializers, List<Expression> encoderFinalizers) {
		StaticEncoders staticEncoders = staticEncoders(classBuilder);

		classBuilder.withMethod("encode", int.class, List.of(byte[].class, int.class, Object.class), methodBody(
				encoderInitializers, encoderFinalizers,
				let(cast(arg(2), serializer.getEncodeType()), data ->
						encoderImpl(serializer, encodeVersion, staticEncoders, arg(0), arg(1), data))));

		classBuilder.withMethod("encode", void.class, List.of(BinaryOutput.class, Object.class), methodBody(
				encoderInitializers, encoderFinalizers,
				let(call(arg(0), "array"), buf ->
						let(call(arg(0), "pos"), pos ->
								let(cast(arg(1), serializer.getEncodeType()), data ->
										sequence(
												encoderImpl(serializer, encodeVersion, staticEncoders, buf, pos, data),
												call(arg(0), "pos", pos)))))));
	}

	private Expression encoderImpl(SerializerDef serializer, @Nullable Integer encodeVersion, StaticEncoders staticEncoders, Expression buf, Variable pos, Variable data) {
		return sequence(
				encodeVersion != null ?
						writeByte(buf, pos, value((byte) (int) encodeVersion)) :
						voidExp(),

				serializer.encoder(staticEncoders,
						buf, pos, data,
						encodeVersion != null ? encodeVersion : 0,
						compatibilityLevel),

				pos);
	}

	private void defineDecoders(ClassBuilder<?> classBuilder, SerializerDef serializer, List<Integer> decodeVersions,
			List<Expression> decoderInitializers, List<Expression> decoderFinalizers) {
		StaticDecoders staticDecoders = staticDecoders(classBuilder);

		Integer latestVersion = decodeVersions.isEmpty() ? null : decodeVersions.get(decodeVersions.size() - 1);
		classBuilder.withMethod("decode", Object.class, List.of(BinaryInput.class), methodBody(
				decoderInitializers, decoderFinalizers,
				decodeImpl(serializer, latestVersion, staticDecoders, arg(0))));

		classBuilder.withMethod("decode", Object.class, List.of(byte[].class, int.class), methodBody(
				decoderInitializers, decoderFinalizers,
				let(constructor(BinaryInput.class, arg(0), arg(1)), in ->
						decodeImpl(serializer, latestVersion, staticDecoders, in))));

		classBuilder.withMethod("decodeEarlierVersions",
				serializer.getDecodeType(),
				List.of(BinaryInput.class, byte.class),
				get(() -> {
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
				}));

		for (int i = decodeVersions.size() - 2; i >= 0; i--) {
			int version = decodeVersions.get(i);
			classBuilder.withMethod("decodeVersion" + version, serializer.getDecodeType(), List.of(BinaryInput.class),
					sequence(serializer.defineDecoder(staticDecoders,
							arg(0), version, compatibilityLevel)));
		}
	}

	private Expression methodBody(List<Expression> initializers, List<Expression> finalizers, Expression body) {
		if (initializers.isEmpty() && finalizers.isEmpty()) return body;
		return finalizers.isEmpty() ?
				sequence(sequence(initializers), body) :
				sequence(sequence(initializers), let(body, v -> sequence(sequence(finalizers), v)));
	}

	private Expression decodeImpl(SerializerDef serializer, Integer latestVersion, StaticDecoders staticDecoders,
			Expression in) {
		return latestVersion == null ?
				serializer.decoder(
						staticDecoders,
						in,
						0,
						compatibilityLevel) :

				let(readByte(in),
						version -> ifEq(version, value((byte) (int) latestVersion),
								serializer.decoder(
										staticDecoders,
										in,
										latestVersion,
										compatibilityLevel),
								call(self(), "decodeEarlierVersions", in, version)));
	}

	private static StaticEncoders staticEncoders(ClassBuilder<?> classBuilder) {
		return new StaticEncoders() {
			final Map<List<?>, String> defined = new HashMap<>();

			@Override
			public Expression define(SerializerDef serializerDef, Class<?> valueClazz, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
				List<?> key = List.of(identityHashCode(serializerDef), version, compatibilityLevel);
				String methodName = defined.get(key);
				if (methodName == null) {
					for (int i = 1; ; i++) {
						methodName = "encode_" +
								valueClazz.getSimpleName().replace('[', 's').replace(']', '_') +
								(i == 1 ? "" : "_" + i);
						if (defined.values().stream().noneMatch(methodName::equals)) break;
					}
					defined.put(key, methodName);
					classBuilder.withStaticMethod(methodName, int.class, List.of(byte[].class, int.class, valueClazz), sequence(
							serializerDef.encoder(this, BUF, POS, VALUE, version, compatibilityLevel),
							POS));
				}
				return set(pos, staticCallSelf(methodName, buf, pos, cast(value, valueClazz)));
			}
		};
	}

	private StaticDecoders staticDecoders(ClassBuilder<?> classBuilder) {
		return new StaticDecoders() {
			final Map<List<?>, String> defined = new HashMap<>();

			@Override
			public Expression define(SerializerDef serializerDef, Class<?> valueClazz, Expression in, int version, CompatibilityLevel compatibilityLevel) {
				List<?> key = List.of(identityHashCode(serializerDef), version, compatibilityLevel);
				String methodName = defined.get(key);
				if (methodName == null) {
					for (int i = 1; ; i++) {
						methodName = "decode_" +
								valueClazz.getSimpleName().replace('[', 's').replace(']', '_') +
								("_V" + version) +
								(i == 1 ? "" : "_" + i);
						if (defined.values().stream().noneMatch(methodName::equals)) break;
					}
					defined.put(key, methodName);
					classBuilder.withStaticMethod(methodName, valueClazz, List.of(BinaryInput.class),
							serializerDef.decoder(this, IN, version, compatibilityLevel));
				}
				return staticCallSelf(methodName, in);
			}

			@Override
			public <T> Class<T> buildClass(ClassBuilder<T> classBuilder) {
				return classBuilder.defineClass(classLoader);
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

		if (hasAnnotation(SerializeReference.class, ctx.getAnnotations()) ||
				hasAnnotation(SerializeReference.class, ctx.getRawType().getAnnotations())) {
			cache.remove(ctx.getType(), forwardingSerializerDef);
		}

		return serializer;
	}

	private SerializerDef doScan(Context<SerializerDef> ctx) {
		Class<?> rawClass = ctx.getRawType();
		if (rawClass.isAnonymousClass())
			throw new IllegalArgumentException("Class should not be anonymous");
		if (rawClass.isLocalClass())
			throw new IllegalArgumentException("Class should not be local");

		SerializerDefClass serializer = SerializerDefClass.create(rawClass);
		if (rawClass.getAnnotation(SerializeRecord.class) != null) {
			if (!rawClass.isRecord()) {
				throw new IllegalArgumentException("Non-record type '" + rawClass.getName() +
						"' annotated with @SerializeRecord annotation");
			}
			scanRecord(ctx, serializer);
		} else if (!rawClass.isInterface()) {
			scanClass(ctx, serializer);
		} else {
			scanInterface(ctx, serializer);
		}
		return serializer;
	}

	private void scanClass(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		AnnotatedType annotatedClassType = ctx.getAnnotatedType();

		Class<?> rawClassType = getRawType(annotatedClassType);
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(annotatedClassType)::get;

		if (rawClassType.getSuperclass() != Object.class) {
			scanClass(ctx.push(bind(rawClassType.getAnnotatedSuperclass(), bindings)), serializer);
		}

		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanConstructors(ctx, serializer);
		scanStaticFactoryMethods(ctx, serializer);
		scanSetters(ctx, serializer);
		scanFields(ctx, bindings, foundSerializers);
		scanGetters(ctx, bindings, foundSerializers);
		addMethodsAndGettersToClass(ctx, serializer, foundSerializers);
		if (!Modifier.isAbstract(ctx.getRawType().getModifiers())) {
			serializer.addMatchingSetters();
		}
	}

	private void scanInterface(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(ctx.getAnnotatedType())::get;

		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanGetters(ctx, bindings, foundSerializers);
		addMethodsAndGettersToClass(ctx, serializer, foundSerializers);

		for (AnnotatedType superInterface : ctx.getRawType().getAnnotatedInterfaces()) {
			scanInterface(ctx.push(bind(superInterface, bindings)), serializer);
		}
	}

	private void scanRecord(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(ctx.getAnnotatedType())::get;
		List<FoundSerializer> foundSerializers = new ArrayList<>();

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
			FoundSerializer foundSerializer = new FoundSerializer(method, order++, Serialize.DEFAULT_VERSION, Serialize.DEFAULT_VERSION);
			foundSerializer.serializer = ctx.scan(bind(
					annotationsCompatibilityMode ?
							annotateWithTypePath(recordComponent.getGenericType(), recordComponent.getAnnotations()) :
							recordComponent.getAnnotatedType(),
					bindings));
			foundSerializers.add(foundSerializer);
		}
		serializer.setConstructor(rawType.getConstructors()[0], Arrays.stream(rawType.getRecordComponents()).map(RecordComponent::getName).toList());
		addMethodsAndGettersToClass(ctx, serializer, foundSerializers);
	}

	private void addMethodsAndGettersToClass(Context<SerializerDef> ctx, SerializerDefClass serializer, List<FoundSerializer> foundSerializers) {
		if (foundSerializers.stream().anyMatch(f -> f.order == Integer.MIN_VALUE)) {
			Map<String, List<FoundSerializer>> foundFields = foundSerializers.stream().collect(groupingBy(FoundSerializer::getName, toList()));
			String pathToClass = ctx.getRawType().getName().replace('.', '/') + ".class";
			try (InputStream classInputStream = ctx.getRawType().getClassLoader().getResourceAsStream(pathToClass)) {
				ClassReader cr = new ClassReader(requireNonNull(classInputStream));
				cr.accept(new ClassVisitor(Opcodes.ASM8) {
					int index = 0;

					@Override
					public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
						List<FoundSerializer> list = foundFields.get(name);
						if (list == null) return null;
						for (FoundSerializer foundSerializer : list) {
							if (!(foundSerializer.methodOrField instanceof Field)) continue;
							foundSerializer.index = index++;
							break;
						}
						return null;
					}

					@Override
					public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
						List<FoundSerializer> list = foundFields.get(name);
						if (list == null) return null;
						for (FoundSerializer foundSerializer : list) {
							if (!(foundSerializer.methodOrField instanceof Method)) continue;
							if (!descriptor.equals(getType((Method) foundSerializer.methodOrField).getDescriptor()))
								continue;
							foundSerializer.index = index++;
							break;
						}
						return null;
					}
				}, SKIP_CODE | SKIP_DEBUG | SKIP_FRAMES);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			for (FoundSerializer foundSerializer : foundSerializers) {
				if (foundSerializer.order == Integer.MIN_VALUE) {
					foundSerializer.order = autoOrderingStart + foundSerializer.index * autoOrderingStride;
				}
			}
		}

		Set<Integer> orders = new HashSet<>();
		for (FoundSerializer foundSerializer : foundSerializers) {
			if (!orders.add(foundSerializer.order))
				throw new IllegalArgumentException(format("Duplicate order %s for %s in %s", foundSerializer.order, foundSerializer, serializer));
		}

		Collections.sort(foundSerializers);
		for (FoundSerializer foundSerializer : foundSerializers) {
			if (foundSerializer.methodOrField instanceof Method) {
				serializer.addGetter((Method) foundSerializer.methodOrField, foundSerializer.serializer, foundSerializer.added, foundSerializer.removed);
			} else if (foundSerializer.methodOrField instanceof Field) {
				serializer.addField((Field) foundSerializer.methodOrField, foundSerializer.serializer, foundSerializer.added, foundSerializer.removed);
			} else {
				throw new AssertionError();
			}
		}
	}

	private void scanFields(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			List<FoundSerializer> foundSerializers) {
		for (Field field : ctx.getRawType().getDeclaredFields()) {
			FoundSerializer foundSerializer = tryAddField(ctx, bindings, field);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanGetters(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			List<FoundSerializer> foundSerializers) {
		for (Method method : ctx.getRawType().getDeclaredMethods()) {
			FoundSerializer foundSerializer = tryAddGetter(ctx, bindings, method);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanSetters(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		for (Method method : ctx.getRawType().getDeclaredMethods()) {
			if (isStatic(method.getModifiers())) {
				continue;
			}
			if (method.getParameterTypes().length != 0) {
				List<String> fields = new ArrayList<>(method.getParameterTypes().length);
				for (int i = 0; i < method.getParameterTypes().length; i++) {
					Annotation[] parameterAnnotations = method.getParameterAnnotations()[i];
					Deserialize annotation = getAnnotation(Deserialize.class, parameterAnnotations);
					if (annotation != null) {
						String field = annotation.value();
						fields.add(field);
					}
				}
				if (fields.size() == method.getParameterTypes().length) {
					serializer.addSetter(method, fields);
				} else {
					if (!fields.isEmpty())
						throw new IllegalArgumentException("Fields should not be empty");
				}
			}
		}
	}

	private void scanStaticFactoryMethods(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		Class<?> factoryClassType = ctx.getRawType();
		for (Method factory : factoryClassType.getDeclaredMethods()) {
			if (ctx.getRawType() != factory.getReturnType()) {
				continue;
			}
			if (factory.getParameterTypes().length != 0) {
				List<String> fields = new ArrayList<>(factory.getParameterTypes().length);
				for (int i = 0; i < factory.getParameterTypes().length; i++) {
					Annotation[] parameterAnnotations = factory.getParameterAnnotations()[i];
					Deserialize annotation = getAnnotation(Deserialize.class, parameterAnnotations);
					if (annotation != null) {
						String field = annotation.value();
						fields.add(field);
					}
				}
				if (fields.size() == factory.getParameterTypes().length) {
					serializer.setStaticFactoryMethod(factory, fields);
				} else {
					if (!fields.isEmpty())
						throw new IllegalArgumentException(format("@Deserialize is not fully specified for %s", fields));
				}
			}
		}
	}

	private void scanConstructors(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		boolean found = false;
		for (Constructor<?> constructor : ctx.getRawType().getDeclaredConstructors()) {
			List<String> fields = new ArrayList<>(constructor.getParameterTypes().length);
			for (int i = 0; i < constructor.getParameterTypes().length; i++) {
				Deserialize annotation = getAnnotation(Deserialize.class, constructor.getParameterAnnotations()[i]);
				if (annotation != null) {
					String field = annotation.value();
					fields.add(field);
				}
			}
			if (constructor.getParameterTypes().length != 0 && fields.size() == constructor.getParameterTypes().length) {
				if (found)
					throw new IllegalArgumentException(format("Duplicate @Deserialize constructor %s", constructor));
				found = true;
				serializer.setConstructor(constructor, fields);
			} else {
				if (!fields.isEmpty())
					throw new IllegalArgumentException(format("@Deserialize is not fully specified for %s", fields));
			}
		}
	}

	private FoundSerializer tryAddField(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			Field field) {
		SerializerBuilder.FoundSerializer result = findAnnotations(field, field.getAnnotations());
		if (result == null) {
			return null;
		}
		if (!isPublic(field.getModifiers()))
			throw new IllegalArgumentException(format("Field %s must be public", field));
		if (isStatic(field.getModifiers()))
			throw new IllegalArgumentException(format("Field %s must not be static", field));
		if (isTransient(field.getModifiers()))
			throw new IllegalArgumentException(format("Field %s must not be transient", field));
		result.serializer = ctx.scan(bind(
				annotationsCompatibilityMode ?
						annotateWithTypePath(field.getGenericType(), field.getAnnotations()) :
						field.getAnnotatedType(),
				bindings));
		return result;
	}

	private @Nullable FoundSerializer tryAddGetter(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			Method getter) {
		if (getter.isBridge()) {
			return null;
		}
		FoundSerializer result = findAnnotations(getter, getter.getAnnotations());
		if (result == null) {
			return null;
		}
		if (!isPublic(getter.getModifiers()))
			throw new IllegalArgumentException(format("Getter %s must be public", getter));
		if (isStatic(getter.getModifiers()))
			throw new IllegalArgumentException(format("Getter %s must not be static", getter));
		if (getter.getReturnType() == Void.TYPE || getter.getParameterTypes().length != 0)
			throw new IllegalArgumentException(format("%s must be getter", getter));
		result.serializer = ctx.scan(bind(
				annotationsCompatibilityMode ?
						annotateWithTypePath(getter.getGenericReturnType(), getter.getAnnotations()) :
						getter.getAnnotatedReturnType(),
				bindings));
		return result;
	}

	private static final Map<Class<? extends Annotation>, Function<Annotation, ? extends Annotation[]>> REPEATABLES_VALUE = new HashMap<>();
	private static final Map<Class<? extends Annotation>, Function<Annotation, int[]>> ANNOTATIONS_PATH = new HashMap<>();

	static {
		repeatable(SerializeFixedSizes.class, SerializeFixedSize.class, SerializeFixedSizes::value, SerializeFixedSize::path);
		repeatable(SerializeNullables.class, SerializeNullable.class, SerializeNullables::value, SerializeNullable::path);
		repeatable(SerializeReferences.class, SerializeReference.class, SerializeReferences::value, SerializeReference::path);
		repeatable(SerializeStringFormats.class, SerializeStringFormat.class, SerializeStringFormats::value, SerializeStringFormat::path);
		repeatable(SerializeClasses.class, SerializeClass.class, SerializeClasses::value, SerializeClass::path);
		repeatable(SerializeVarLengths.class, SerializeVarLength.class, SerializeVarLengths::value, SerializeVarLength::path);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static <AR extends Annotation, AV extends Annotation> void repeatable(Class<AR> arClass, Class<AV> avClass,
			Function<AR, ? extends AV[]> toValue, Function<AV, int[]> toPath) {
		REPEATABLES_VALUE.put(arClass, (Function) toValue);
		ANNOTATIONS_PATH.put(avClass, (Function) toPath);
	}

	private static AnnotatedType annotateWithTypePath(Type type, Annotation[] annotations) {
		return annotatedTypeOf(type, ($, path) -> {
			List<Annotation> result = new ArrayList<>();
			for (Annotation annotation : annotations) {
				for (Annotation a : REPEATABLES_VALUE.getOrDefault(annotation.annotationType(), ar -> new Annotation[]{ar}).apply(annotation)) {
					if (Arrays.equals(ANNOTATIONS_PATH.getOrDefault(a.annotationType(), av -> null).apply(a), path)) {
						result.add(a);
					}
				}
			}
			return result.toArray(new Annotation[0]);
		});
	}

	private @Nullable FoundSerializer findAnnotations(Object methodOrField, Annotation[] annotations) {
		int added = Serialize.DEFAULT_VERSION;
		int removed = Serialize.DEFAULT_VERSION;

		Serialize serialize = getAnnotation(Serialize.class, annotations);
		if (serialize != null) {
			added = serialize.added();
			removed = serialize.removed();
		}

		SerializeProfiles profiles = getAnnotation(SerializeProfiles.class, annotations);
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

		return serialize != null ? new FoundSerializer(methodOrField, serialize.order(), added, removed) : null;
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

	private static final class FoundSerializer implements Comparable<FoundSerializer> {
		final Object methodOrField;
		int order;
		int index;
		final int added;
		final int removed;
		//		final TypedModsMap mods;
		SerializerDef serializer;

		private FoundSerializer(Object methodOrField, int order, int added, int removed) {
			this.methodOrField = methodOrField;
			this.order = order;
			this.added = added;
			this.removed = removed;
		}

		public String getName() {
			if (methodOrField instanceof Field) {
				return ((Field) methodOrField).getName();
			}
			if (methodOrField instanceof Method) {
				return ((Method) methodOrField).getName();
			}
			throw new AssertionError();
		}

		private int fieldRank() {
			if (methodOrField instanceof Field) {
				return 1;
			}
			if (methodOrField instanceof Method) {
				return 2;
			}
			throw new AssertionError();
		}

		@Override
		public int compareTo(FoundSerializer o) {
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
			return methodOrField.getClass().getSimpleName() + " " + getName();
		}
	}

	static class ForwardingSerializerDefImpl extends ForwardingSerializerDef {
		SerializerDef serializerDef;

		@Override
		protected SerializerDef serializer() {
			return serializerDef;
		}
	}

}
