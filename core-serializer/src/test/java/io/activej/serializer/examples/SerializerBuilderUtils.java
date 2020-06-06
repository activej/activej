package io.activej.serializer.examples;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class SerializerBuilderUtils {
	public static final List<Class<?>> TYPES = asList(
			byte.class, short.class, int.class, long.class, float.class, double.class, char.class, Object.class
	);
	private static final Map<Class<?>, SerializerDef> primitiveSerializers = new HashMap<Class<?>, SerializerDef>() {{
		put(byte.class, new SerializerDefByte(false));
		put(short.class, new SerializerDefShort(false));
		put(int.class, new SerializerDefInt(false, true));
		put(long.class, new SerializerDefLong(false, false));
		put(float.class, new SerializerDefFloat(false));
		put(double.class, new SerializerDefDouble(false));
		put(char.class, new SerializerDefChar(false));
	}};

	private static final Map<String, String> collectionImplSuffix = new HashMap<String, String>() {{
		put("Set", "HashSet");
		put("IndexedContainer", "ArrayList");
	}};

	// region creators
	public static SerializerBuilder createWithHppc7Support(String profile, DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = SerializerBuilder.create(profile, definingClassLoader);
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
					throw new IllegalStateException("There is no collection with given name" + e.getClass().getName(), e);
				}
				builder.withSerializer(hppcMapType, serializerDefMapBuilder(hppcMapType, hppcMapImplType, keyType, valueType));
			}
		}
	}

	private static void registerHppcCollections(SerializerBuilder builder, DefiningClassLoader classLoader) {
		for (Map.Entry<String, String> collectionImpl : collectionImplSuffix.entrySet()) {
			for (Class<?> valueType : TYPES) {
				String valueTypeName = valueType.getSimpleName();
				String prefix = "com.carrotsearch.hppc." + capitalize(valueTypeName);
				String hppcCollectionTypeName = prefix + collectionImpl.getKey();
				String hppcCollectionTypeImplName = prefix + collectionImpl.getValue();
				Class<?> hppcCollectionType, hppcCollectionTypeImpl;
				try {
					hppcCollectionType = Class.forName(hppcCollectionTypeName, true, classLoader);
					hppcCollectionTypeImpl = Class.forName(hppcCollectionTypeImplName, true, classLoader);
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("There is no collection with given name", e);
				}
				builder.withSerializer(hppcCollectionType, serializerDefCollectionBuilder(hppcCollectionType, hppcCollectionTypeImpl, valueType));
			}
		}
	}

	public static String capitalize(String str) {
		return toUpperCase(str.charAt(0)) + str.substring(1);
	}

	private static SerializerDefBuilder serializerDefMapBuilder(Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
		if (!mapType.getSimpleName().startsWith(prefix))
			throw new IllegalArgumentException(format("Expected mapType '%s', but was begin '%s'", mapType.getSimpleName(), prefix));
		return (type, generics, target) -> {
			SerializerDef keySerializer;
			SerializerDef valueSerializer;
			if (generics.length == 2) {
				if (keyType != Object.class || valueType != Object.class)
					throw new IllegalArgumentException("keyType and valueType must be Object.class");
				keySerializer = generics[0].serializer;
				valueSerializer = generics[1].serializer;
			} else if (generics.length == 1) {
				if (keyType != Object.class && valueType != Object.class)
					throw new IllegalArgumentException("keyType or valueType must be Object.class");
				if (keyType == Object.class) {
					keySerializer = generics[0].serializer;
					valueSerializer = primitiveSerializers.get(valueType);
				} else {
					keySerializer = primitiveSerializers.get(keyType);
					valueSerializer = generics[0].serializer;
				}
			} else {
				keySerializer = primitiveSerializers.get(keyType);
				valueSerializer = primitiveSerializers.get(valueType);
			}
			if (valueSerializer == null)
				throw new NullPointerException();
			if (keySerializer == null)
				throw new NullPointerException();
			return new SerializerDefHppc7Map(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType);
		};
	}

	private static SerializerDefBuilder serializerDefCollectionBuilder(Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		String prefix = capitalize(valueType.getSimpleName());
		if (!collectionType.getSimpleName().startsWith(prefix))
			throw new IllegalArgumentException(format("Expected setType '%s', but was begin '%s'", collectionType.getSimpleName(), prefix));
		return (type, generics, target) -> {
			SerializerDef valueSerializer;
			if (generics.length == 1) {
				if (valueType != Object.class)
					throw new IllegalArgumentException("valueType must be Object.class");
				valueSerializer = generics[0].serializer;
			} else {
				valueSerializer = primitiveSerializers.get(valueType);
			}
			if (valueSerializer == null)
				throw new NullPointerException();
			return new SerializerDefHppc7Collection(collectionType, collectionImplType, valueType, valueSerializer);
		};
	}
}
