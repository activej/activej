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

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.annotations.*;
import io.activej.serializer.impl.*;
import io.activej.serializer.reflection.TypeT;
import io.activej.serializer.reflection.scanner.TypeScannerRegistry;
import io.activej.serializer.reflection.scanner.TypeScannerRegistry.Context;
import io.activej.serializer.reflection.scanner.TypeScannerRegistry.Mapping;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.codegen.util.Utils.getPathSetting;
import static io.activej.serializer.SerializerDef.*;
import static io.activej.serializer.impl.SerializerExpressions.readByte;
import static io.activej.serializer.impl.SerializerExpressions.writeByte;
import static io.activej.serializer.reflection.scanner.TypeUtils.*;
import static io.activej.serializer.util.Utils.*;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.lang.reflect.Modifier.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.newSetFromMap;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toList;

/**
 * Scans fields of classes for serialization.
 */
@SuppressWarnings({"unused"})
public final class SerializerBuilder {
	private static final Path DEFAULT_SAVE_DIR = getPathSetting(SerializerBuilder.class, "saveDir", null);

	private final DefiningClassLoader classLoader;

	private final TypeScannerRegistry<SerializerDef> registry = TypeScannerRegistry.create();

	private String profile;
	private int encodeVersionMax = Integer.MAX_VALUE;
	private int decodeVersionMin = 0;
	private int decodeVersionMax = Integer.MAX_VALUE;
	private Path saveBytecodePath = DEFAULT_SAVE_DIR;
	private CompatibilityLevel compatibilityLevel = CompatibilityLevel.LEVEL_3;
	private Object[] classKey = null;

	private final Map<Object, List<Class<?>>> extraSubclassesMap = new HashMap<>();

	public SerializerBuilder(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public static SerializerBuilder create() {
		return create(DefiningClassLoader.create());
	}

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
				new TypeT<Object[]>() {}.getType()}) {
			builder.with(type, ctx -> new SerializerDefArray(ctx.scanTypeArgument(0), ctx.getRawClass()));
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
					for (Method method : ctx.getRawClass().getDeclaredMethods()) {
						if (hasAnnotation(Serialize.class, method.getAnnotations())) {
							return builder.scan(ctx);
						}
					}
					for (Field field : ctx.getRawClass().getDeclaredFields()) {
						if (hasAnnotation(Serialize.class, field.getAnnotations())) {
							return builder.scan(ctx);
						}
					}
					//noinspection unchecked
					return new SerializerDefEnum((Class<? extends Enum<?>>) ctx.getRawClass());
				})

				.with(String.class, ctx -> new SerializerDefString(
						ctx.getAnnotation(SerializeStringFormat.class, a -> a == null ? StringFormat.UTF8 : a.value())))

				.with(Collection.class, ctx -> new SerializerDefCollection(ctx.scanTypeArgument(0), Collection.class, ArrayList.class))
				.with(List.class, ctx -> new SerializerDefList(ctx.scanTypeArgument(0)))
				.with(Queue.class, ctx -> new SerializerDefCollection(ctx.scanTypeArgument(0), Queue.class, ArrayDeque.class))
				.with(Map.class, ctx -> new SerializerDefMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1)))
				.with(HashMap.class, ctx -> new SerializerDefMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1), HashMap.class, HashMap.class))
				.with(LinkedHashMap.class, ctx -> new SerializerDefMap(ctx.scanTypeArgument(0), ctx.scanTypeArgument(1), LinkedHashMap.class, LinkedHashMap.class))
				.with(Set.class, ctx -> new SerializerDefSet(ctx.scanTypeArgument(0)))
				.with(HashSet.class, ctx -> new SerializerDefCollection(ctx.scanTypeArgument(0), HashSet.class, HashSet.class))
				.with(LinkedHashSet.class, ctx -> new SerializerDefCollection(ctx.scanTypeArgument(0), LinkedHashSet.class, LinkedHashSet.class))

				.with(Object.class, builder::scan);

		return builder;
	}

	public SerializerBuilder with(TypeT<?> typeT, Mapping<SerializerDef> fn) {
		return with(typeT.getType(), fn);
	}

	@SuppressWarnings("PointlessBooleanExpression")
	public SerializerBuilder with(Type type, Mapping<SerializerDef> fn) {
		registry.with(type, ctx -> {
			Class<?> rawClass = ctx.getRawClass();
			SerializerDef serializerDef;
			SerializeClass annotationClass;
			SerializeSubclasses annotationSubclass;
			if (false ||
					(annotationClass = ctx.getAnnotation(SerializeClass.class)) != null ||
					(annotationClass = getAnnotation(SerializeClass.class, rawClass.getAnnotations())) != null) {
				try {
					serializerDef = annotationClass.value().getDeclaredConstructor().newInstance();
				} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
					try {
						serializerDef = (SerializerDef) annotationClass.value().getMethod("instance").invoke(null);
					} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException ignored) {
						throw new RuntimeException(e);
					}
				}
			} else if (false ||
					(annotationSubclass = ctx.getAnnotation(SerializeSubclasses.class)) != null ||
					(annotationSubclass = getAnnotation(SerializeSubclasses.class, rawClass.getAnnotations())) != null) {
				LinkedHashMap<Class<?>, SerializerDef> map = new LinkedHashMap<>();
				LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(Arrays.asList(annotationSubclass.value()));
				subclassesSet.addAll(extraSubclassesMap.getOrDefault(rawClass, emptyList()));
				subclassesSet.addAll(extraSubclassesMap.getOrDefault(annotationSubclass.extraSubclassesId(), emptyList()));
				for (Class<?> subclass : subclassesSet) {
					map.put(subclass, ctx.scan(subclass));
				}
				serializerDef = new SerializerDefSubclass(rawClass, map, annotationSubclass.startIndex());
			} else {
				serializerDef = fn.apply(ctx);
			}

			if (ctx.hasAnnotation(SerializeReference.class) ||
					hasAnnotation(SerializeReference.class, rawClass.getAnnotations())) {
				serializerDef = new SerializerDefReference(serializerDef);
			}

			if (ctx.hasAnnotation(SerializeVarLength.class)) {
				serializerDef = ((SerializerDefWithVarLength) serializerDef).ensureVarLength();
			}

			SerializeFixedSize annotationFixedSize;
			if ((annotationFixedSize = ctx.getAnnotation(SerializeFixedSize.class)) != null) {
				serializerDef = ((SerializerDefWithFixedSize) serializerDef).ensureFixedSize(annotationFixedSize.value());
			}

			if (ctx.hasAnnotation(SerializeNullable.class)) {
				serializerDef = serializerDef instanceof SerializerDefWithNullable ?
						((SerializerDefWithNullable) serializerDef).ensureNullable() : new SerializerDefNullable(serializerDef);
			}

			return serializerDef;
		});
		return this;
	}

	public SerializerBuilder withSubclasses(String extraSubclassesId, Class<?>... subclasses) {
		return withSubclasses(extraSubclassesId, Arrays.asList(subclasses));
	}

	public SerializerBuilder withSubclasses(String subclassesId, List<Class<?>> subclasses) {
		extraSubclassesMap.put(subclassesId, subclasses);
		return this;
	}

	public SerializerBuilder withCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
		this.compatibilityLevel = compatibilityLevel;
		return this;
	}

	/**
	 * Allows to save generated bytecode in file at provided {@code path}
	 *
	 * @param path defines where generated bytecode will be stored
	 */
	public SerializerBuilder withGeneratedBytecodePath(Path path) {
		this.saveBytecodePath = path;
		return this;
	}

	public SerializerBuilder withEncodeVersion(int encodeVersionMax) {
		this.encodeVersionMax = encodeVersionMax;
		return this;
	}

	public SerializerBuilder withDecodeVersions(int decodeVersionMin, int decodeVersionMax) {
		this.decodeVersionMin = decodeVersionMin;
		this.decodeVersionMax = decodeVersionMax;
		return this;
	}

	public SerializerBuilder withVersions(int encodeVersionMax, int decodeVersionMin, int decodeVersionMax) {
		this.encodeVersionMax = encodeVersionMax;
		this.decodeVersionMin = decodeVersionMin;
		this.decodeVersionMax = decodeVersionMax;
		return this;
	}

	public SerializerBuilder withProfile(String profile) {
		this.profile = profile;
		return this;
	}

	public SerializerBuilder withClassKey(Object... classKeyParameters) {
		this.classKey = classKeyParameters;
		return this;
	}

	public void setSubclasses(String subclassesId, List<Class<?>> subclasses) {
		extraSubclassesMap.put(subclassesId, subclasses);
	}

	public <T> SerializerBuilder withSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
		setSubclasses(type, subclasses);
		return this;
	}

	public <T> void setSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
		//noinspection unchecked,rawtypes
		extraSubclassesMap.put(type, (List) subclasses);
	}

	public <T> BinarySerializer<T> build(Type type) {
		return build(annotatedTypeOf(type));
	}

	public <T> BinarySerializer<T> build(AnnotatedType type) {
		return build(registry.scanner(new HashMap<>()).scan(type));
	}

	public <T> BinarySerializer<T> build(SerializerDef serializer) {
		//noinspection rawtypes
		ClassBuilder<BinarySerializer> classBuilder = ClassBuilder.create(classLoader, BinarySerializer.class).withClassKey(classKey);

		if (saveBytecodePath != null) {
			classBuilder.withBytecodeSaveDir(saveBytecodePath);
		}

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
				encoderInitializers.putAll(visitedSerializer.getEncoderInitializer());
				decoderInitializers.putAll(visitedSerializer.getDecoderInitializer());
				encoderFinalizers.putAll(visitedSerializer.getEncoderFinalizer());
				decoderFinalizers.putAll(visitedSerializer.getDecoderFinalizer());
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

		defineEncoders(classBuilder, serializer, encodeVersion,
				new ArrayList<>(encoderInitializers.values()), new ArrayList<>(encoderFinalizers.values()));

		defineDecoders(classBuilder, serializer, decodeVersions,
				new ArrayList<>(decoderInitializers.values()), new ArrayList<>(decoderFinalizers.values()));

		//noinspection unchecked
		return (BinarySerializer<T>) classBuilder.buildClassAndCreateNewInstance();
	}

	private void defineEncoders(ClassBuilder<?> classBuilder, SerializerDef serializer, @Nullable Integer encodeVersion,
			List<Expression> encoderInitializers, List<Expression> encoderFinalizers) {
		StaticEncoders staticEncoders = staticEncoders(classBuilder);

		classBuilder.withMethod("encode", int.class, asList(byte[].class, int.class, Object.class), methodBody(
				encoderInitializers, encoderFinalizers,
				let(cast(arg(2), serializer.getEncodeType()), data ->
						encoderImpl(classBuilder, serializer, encodeVersion, staticEncoders, arg(0), arg(1), data))));

		classBuilder.withMethod("encode", void.class, asList(BinaryOutput.class, Object.class), methodBody(
				encoderInitializers, encoderFinalizers,
				let(call(arg(0), "array"), buf ->
						let(call(arg(0), "pos"), pos ->
								let(cast(arg(1), serializer.getEncodeType()), data ->
										sequence(
												encoderImpl(classBuilder, serializer, encodeVersion, staticEncoders, buf, pos, data),
												call(arg(0), "pos", pos)))))));
	}

	private Expression encoderImpl(ClassBuilder<?> classBuilder, SerializerDef serializer, @Nullable Integer encodeVersion, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression data) {
		return sequence(
				encodeVersion != null ?
						writeByte(buf, pos, value((byte) (int) encodeVersion)) :
						sequence(),

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
		classBuilder.withMethod("decode", Object.class, asList(BinaryInput.class), methodBody(
				decoderInitializers, decoderFinalizers,
				decodeImpl(classBuilder, serializer, latestVersion, staticDecoders, arg(0))));

		classBuilder.withMethod("decode", Object.class, asList(byte[].class, int.class), methodBody(
				decoderInitializers, decoderFinalizers,
				let(constructor(BinaryInput.class, arg(0), arg(1)), in ->
						decodeImpl(classBuilder, serializer, latestVersion, staticDecoders, in))));

		classBuilder.withMethod("decodeEarlierVersions",
				serializer.getDecodeType(),
				asList(BinaryInput.class, byte.class),
				eval(() -> {
					List<Expression> listKey = new ArrayList<>();
					List<Expression> listValue = new ArrayList<>();
					for (int i = decodeVersions.size() - 2; i >= 0; i--) {
						int version = decodeVersions.get(i);
						listKey.add(value((byte) version));
						listValue.add(call(self(), "decodeVersion" + version, arg(0)));
					}
					return switchByKey(arg(1), listKey, listValue,
							throwException(CorruptedDataException.class,
									concat(
											value("Unsupported version: "), arg(1),
											value(", supported versions: " + extractRanges(decodeVersions)))
							));
				}));

		for (int i = decodeVersions.size() - 2; i >= 0; i--) {
			int version = decodeVersions.get(i);
			classBuilder.withMethod("decodeVersion" + version, serializer.getDecodeType(), asList(BinaryInput.class),
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

	private Expression decodeImpl(ClassBuilder<?> classBuilder, SerializerDef serializer, Integer latestVersion, StaticDecoders staticDecoders,
			Expression in) {
		return latestVersion == null ?
				serializer.decoder(
						staticDecoders,
						in,
						0,
						compatibilityLevel) :

				let(readByte(in),
						version -> ifThenElse(cmpEq(version, value((byte) (int) latestVersion)),
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
				List<?> key = asList(identityHashCode(serializerDef), version, compatibilityLevel);
				String methodName = defined.get(key);
				if (methodName == null) {
					for (int i = 1; ; i++) {
						methodName = "encode_" +
								valueClazz.getSimpleName().replace('[', 's').replace(']', '_') +
								(i == 1 ? "" : "_" + i);
						if (defined.values().stream().noneMatch(methodName::equals)) break;
					}
					defined.put(key, methodName);
					classBuilder.withStaticMethod(methodName, int.class, asList(byte[].class, int.class, valueClazz), sequence(
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
				List<?> key = asList(identityHashCode(serializerDef), version, compatibilityLevel);
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
					classBuilder.withStaticMethod(methodName, valueClazz, asList(BinaryInput.class),
							serializerDef.decoder(this, IN, version, compatibilityLevel));
				}
				return staticCallSelf(methodName, in);
			}

			@Override
			public <T> ClassBuilder<T> buildClass(Class<T> type) {
				return ClassBuilder.create(classLoader, type);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private SerializerDef scan(Context<SerializerDef> ctx) {
		Map<Type, SerializerDef> cache = (Map<Type, SerializerDef>) ctx.value();
		SerializerDef serializerDef = cache.get(ctx.getType());
		if (serializerDef != null) return serializerDef;
		ForwardingSerializerDefImpl forwardingSerializerDef = new ForwardingSerializerDefImpl();
		cache.put(ctx.getType(), forwardingSerializerDef);

		SerializerDef serializer = doScan(ctx);

		forwardingSerializerDef.serializerDef = serializer;
		cache.remove(ctx.getType(), forwardingSerializerDef);
		return serializer;
	}

	private SerializerDef doScan(Context<SerializerDef> ctx) {
		Class<?> rawClass = ctx.getRawClass();
		if (rawClass.isAnonymousClass())
			throw new IllegalArgumentException("Class should not be anonymous");
		if (rawClass.isLocalClass())
			throw new IllegalArgumentException("Class should not be local");

		SerializerDefClass serializer = SerializerDefClass.of(rawClass);
		if (!rawClass.isInterface()) {
			scanClass(ctx, serializer);
		} else {
			scanInterface(ctx, serializer);
		}
		return serializer;
	}

	private void scanClass(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		AnnotatedType annotatedClassType = ctx.getAnnotatedType();

		Class<?> rawClassType = getRawClass(annotatedClassType);
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(annotatedClassType)::get;

		if (rawClassType.getSuperclass() != Object.class) {
			scanClass(ctx.push(bind(rawClassType.getAnnotatedSuperclass(), bindings)), serializer);
		}

		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanFields(ctx, bindings, foundSerializers);
		scanGetters(ctx, bindings, foundSerializers);
		addMethodsAndGettersToClass(serializer, foundSerializers);
		scanSetters(ctx, bindings, serializer);
		scanFactories(ctx, bindings, serializer);
		scanConstructors(ctx, bindings, serializer);
		if (!Modifier.isAbstract(ctx.getRawClass().getModifiers())) {
			serializer.addMatchingSetters();
		}
	}

	private void scanInterface(Context<SerializerDef> ctx, SerializerDefClass serializer) {
		Function<TypeVariable<?>, AnnotatedType> bindings = getTypeBindings(ctx.getAnnotatedType())::get;

		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanGetters(ctx, bindings, foundSerializers);
		addMethodsAndGettersToClass(serializer, foundSerializers);

		for (AnnotatedType superInterface : ctx.getRawClass().getAnnotatedInterfaces()) {
			scanInterface(ctx.push(bind(superInterface, bindings)), serializer);
		}
	}

	private void addMethodsAndGettersToClass(SerializerDefClass serializer, List<FoundSerializer> foundSerializers) {
		Set<Integer> orders = new HashSet<>();
		for (FoundSerializer foundSerializer : foundSerializers) {
			if (foundSerializer.order < 0)
				throw new IllegalArgumentException(format("Invalid order %s for %s in %s", foundSerializer.order, foundSerializer, serializer));
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
		for (Field field : getRawClass(ctx.getAnnotatedType()).getDeclaredFields()) {
			FoundSerializer foundSerializer = tryAddField(ctx, bindings, field);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanGetters(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			List<FoundSerializer> foundSerializers) {
		for (Method method : getRawClass(ctx.getAnnotatedType()).getDeclaredMethods()) {
			FoundSerializer foundSerializer = tryAddGetter(ctx, bindings, method);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanSetters(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			SerializerDefClass serializer) {
		for (Method method : ctx.getRawClass().getDeclaredMethods()) {
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

	private void scanFactories(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			SerializerDefClass serializer) {
		Class<?> factoryClassType = ctx.getRawClass();
		for (Method factory : factoryClassType.getDeclaredMethods()) {
			if (ctx.getRawClass() != factory.getReturnType()) {
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
					serializer.setFactory(factory, fields);
				} else {
					if (!fields.isEmpty())
						throw new IllegalArgumentException(format("@Deserialize is not fully specified for %s", fields));
				}
			}
		}
	}

	private void scanConstructors(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			SerializerDefClass serializer) {
		boolean found = false;
		for (Constructor<?> constructor : ctx.getRawClass().getDeclaredConstructors()) {
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
		SerializerBuilder.FoundSerializer result = findAnnotations(ctx, field, field.getAnnotations());
		if (result == null) {
			return null;
		}
		if (!isPublic(field.getModifiers()))
			throw new IllegalArgumentException(format("Field %s must be public", field));
		if (isStatic(field.getModifiers()))
			throw new IllegalArgumentException(format("Field %s must not be static", field));
		if (isTransient(field.getModifiers()))
			throw new IllegalArgumentException(format("Field %s must not be transient", field));
		result.serializer = ctx.scan(bind(field.getAnnotatedType(), bindings));
		return result;
	}

	@Nullable
	private FoundSerializer tryAddGetter(Context<SerializerDef> ctx, Function<TypeVariable<?>, AnnotatedType> bindings,
			Method getter) {
		if (getter.isBridge()) {
			return null;
		}
		FoundSerializer result = findAnnotations(ctx, getter, getter.getAnnotations());
		if (result == null) {
			return null;
		}
		if (!isPublic(getter.getModifiers()))
			throw new IllegalArgumentException(format("Getter %s must be public", getter));
		if (isStatic(getter.getModifiers()))
			throw new IllegalArgumentException(format("Getter %s must not be static", getter));
		if (getter.getReturnType() == Void.TYPE || getter.getParameterTypes().length != 0)
			throw new IllegalArgumentException(format("%s must be getter", getter));
		result.serializer = ctx.scan(bind(getter.getAnnotatedReturnType(), bindings));
		return result;
	}

	@Nullable
	private FoundSerializer findAnnotations(Context<SerializerDef> ctx,
			Object methodOrField, Annotation[] annotations) {
		int added = Serialize.DEFAULT_VERSION;
		int removed = Serialize.DEFAULT_VERSION;

		Serialize serialize = getAnnotation(Serialize.class, annotations);
		if (serialize != null) {
			added = serialize.added();
			removed = serialize.removed();
		}

		SerializeProfiles profiles = getAnnotation(SerializeProfiles.class, annotations);
		if (profiles != null) {
			if (!Arrays.asList(profiles.value()).contains(profile == null ? "" : profile)) {
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
		final int order;
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
