package io.activej.serializer.examples;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.SerializerDef;
import io.activej.types.scanner.TypeScannerRegistry.Mapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Character.toUpperCase;
import static java.lang.String.format;

public class SerializerBuilderUtils {
	public static final List<Class<?>> TYPES = List.of(
			byte.class, short.class, int.class, long.class, float.class, double.class, char.class, Object.class
	);

	private static final Map<String, String> COLLECTION_IMPL_SUFFIX = new HashMap<>() {{
		put("Set", "HashSet");
		put("IndexedContainer", "ArrayList");
	}};

	// region creators
	public static SerializerBuilder createWithHppc7Support(String profile, DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = SerializerBuilder.create(definingClassLoader).withProfile(profile);
		return register(builder, definingClassLoader);
	}

	public static SerializerBuilder createWithHppc7Support(DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = SerializerBuilder.create(definingClassLoader);
		return register(builder, definingClassLoader);

	}

	// endregion
	private static SerializerBuilder register(SerializerBuilder builder, DefiningClassLoader definingClassLoader) {
		registerHppcMaps(builder, definingClassLoader);
		registerHppcCollections(builder, definingClassLoader);
		return builder;
	}

	private static void registerHppcMaps(SerializerBuilder builder, DefiningClassLoader classLoader) {
		for (int i = 0; i < TYPES.size(); i++) {
			Class<?> keyType = TYPES.get(i);
			if (keyType == float.class || keyType == byte.class || keyType == double.class) {
				continue;
			}
			String keyTypeName = keyType.getSimpleName();
			for (Class<?> valueType : TYPES) {
				String valueTypeName = valueType.getSimpleName();
				String prefix = "com.carrotsearch.hppc." + capitalize(keyTypeName) + capitalize(valueTypeName);
				String hppcMapTypeName = prefix + "Map";
				String hppcMapImplTypeName = prefix + "HashMap";
				Class<?> hppcMapType, hppcMapImplType;
				try {
					hppcMapType = Class.forName(hppcMapTypeName, true, classLoader);
					hppcMapImplType = Class.forName(hppcMapImplTypeName, true, classLoader);
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("There is no collection with given name", e);
				}
				builder.with(hppcMapType, serializerDefMap(hppcMapType, hppcMapImplType, keyType, valueType));
			}
		}
	}

	private static void registerHppcCollections(SerializerBuilder builder, DefiningClassLoader classLoader) {
		for (Map.Entry<String, String> collectionImpl : COLLECTION_IMPL_SUFFIX.entrySet()) {
			for (Class<?> valueType : TYPES) {
				String collectionImplKey = collectionImpl.getKey();
				if (collectionImplKey.equals("Set") &&
						(valueType == byte.class || valueType == float.class || valueType == double.class)) {
					continue;
				}
				String valueTypeName = valueType.getSimpleName();
				String prefix = "com.carrotsearch.hppc." + capitalize(valueTypeName);
				String hppcCollectionTypeName = prefix + collectionImplKey;
				String hppcCollectionTypeImplName = prefix + collectionImpl.getValue();
				Class<?> hppcCollectionType, hppcCollectionTypeImpl;
				try {
					hppcCollectionType = Class.forName(hppcCollectionTypeName, true, classLoader);
					hppcCollectionTypeImpl = Class.forName(hppcCollectionTypeImplName, true, classLoader);
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("There is no collection with given name", e);
				}
				builder.with(hppcCollectionType, serializerDefCollection(hppcCollectionType, hppcCollectionTypeImpl, valueType));
			}
		}
	}

	public static String capitalize(String str) {
		return toUpperCase(str.charAt(0)) + str.substring(1);
	}

	private static Mapping<SerializerDef> serializerDefMap(Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
		if (!mapType.getSimpleName().startsWith(prefix))
			throw new IllegalArgumentException(format("Expected mapType '%s', but was begin '%s'", mapType.getSimpleName(), prefix));
		return ctx -> {
			SerializerDef keySerializer;
			SerializerDef valueSerializer;
			if (ctx.getTypeArgumentsCount() == 2) {
				if (keyType != Object.class || valueType != Object.class)
					throw new IllegalArgumentException("keyType and valueType must be Object.class");
				keySerializer = ctx.scanTypeArgument(0);
				valueSerializer = ctx.scanTypeArgument(1);
			} else if (ctx.getTypeArgumentsCount() == 1) {
				if (keyType != Object.class && valueType != Object.class)
					throw new IllegalArgumentException("keyType or valueType must be Object.class");
				if (keyType == Object.class) {
					keySerializer = ctx.scanTypeArgument(0);
					valueSerializer = ctx.scan(valueType);
				} else {
					keySerializer = ctx.scan(keyType);
					valueSerializer = ctx.scanTypeArgument(0);
				}
			} else {
				keySerializer = ctx.scan(keyType);
				valueSerializer = ctx.scan(valueType);
			}
			if (valueSerializer == null)
				throw new NullPointerException();
			if (keySerializer == null)
				throw new NullPointerException();
			return new SerializerDefHppc7HashMap(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType);
		};
	}

	private static Mapping<SerializerDef> serializerDefCollection(Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		String prefix = capitalize(valueType.getSimpleName());
		if (!collectionType.getSimpleName().startsWith(prefix))
			throw new IllegalArgumentException(format("Expected setType '%s', but was begin '%s'", collectionType.getSimpleName(), prefix));
		boolean isHashSet = collectionImplType.getSimpleName().contains("HashSet");
		return ctx -> {
			SerializerDef valueSerializer;
			if (ctx.hasTypeArguments()) {
				if (valueType != Object.class)
					throw new IllegalArgumentException("valueType must be Object.class");
				valueSerializer = ctx.scanTypeArgument(0);
			} else {
				valueSerializer = ctx.scan(valueType);
			}
			if (valueSerializer == null)
				throw new NullPointerException();
			if (isHashSet) {
				return new SerializerDefHppc7HashSet(valueSerializer, collectionType, collectionImplType, valueType);
			}
			return new SerializerDefHppc7RegularCollection(valueSerializer, collectionType, collectionImplType, valueType);
		};
	}
}
