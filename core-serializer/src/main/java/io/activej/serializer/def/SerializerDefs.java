package io.activej.serializer.def;

import io.activej.common.annotation.StaticFactories;
import io.activej.serializer.StringFormat;
import io.activej.serializer.def.impl.*;
import io.activej.serializer.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import static io.activej.common.Checks.checkArgument;

@StaticFactories(SerializerDef.class)
public class SerializerDefs {

	public static SerializerDef ofArray(SerializerDef componentSerializer) {
		Class<?> componentEncodeType = componentSerializer.getEncodeType();
		Class<?> componentDecodeType = componentSerializer.getDecodeType();

		Class<?> encodeType = getArrayType(componentEncodeType);
		Class<?> decodeType = componentEncodeType == componentDecodeType ?
			encodeType :
			getArrayType(componentDecodeType);
		return new ArraySerializerDef(componentSerializer, -1, false, encodeType, decodeType);
	}

	public static SerializerDef ofBoolean(boolean wrapped) {
		return new BooleanSerializerDef(wrapped, false);
	}

	public static SerializerDef ofChar(boolean wrapped) {
		return new CharSerializerDef(wrapped);
	}

	public static SerializerDef ofByte(boolean wrapped) {
		return new ByteSerializerDef(wrapped);
	}

	public static SerializerDef ofShort(boolean wrapped) {
		return new ShortSerializerDef(wrapped);
	}

	public static SerializerDef ofInt(boolean wrapped) {
		return new IntSerializerDef(wrapped, false);
	}

	public static SerializerDef ofInt(boolean wrapped, boolean varLength) {
		return new IntSerializerDef(wrapped, varLength);
	}

	public static SerializerDef ofLong(boolean wrapped) {
		return new LongSerializerDef(wrapped, false);
	}

	public static SerializerDef ofLong(boolean wrapped, boolean varLength) {
		return new LongSerializerDef(wrapped, varLength);
	}

	public static SerializerDef ofFloat(boolean wrapped) {
		return new FloatSerializerDef(wrapped);
	}

	public static SerializerDef ofDouble(boolean wrapped) {
		return new DoubleSerializerDef(wrapped);
	}

	public static SerializerDef ofString(StringFormat format) {
		return new StringSerializerDef(format, false);
	}

	public static <E extends Enum<E>> SerializerDef ofEnum(Class<E> enumClass) {
		return new EnumSerializerDef(enumClass, false);
	}

	public static SerializerDef ofInet4Address() {
		return new Inet4AddressSerializerDef();
	}

	public static SerializerDef ofInet6Address() {
		return new Inet6AddressSerializerDef();
	}

	public static SerializerDef ofInetAddress() {
		SubclassSerializerDef.Builder subclassBuilder = SubclassSerializerDef.builder(InetAddress.class);
		subclassBuilder.withSubclass(Inet4Address.class, new Inet4AddressSerializerDef());
		subclassBuilder.withSubclass(Inet6Address.class, new Inet6AddressSerializerDef());
		return subclassBuilder.build();
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofCollection(
		SerializerDef elementSerializer, Class<? extends Collection> collectionType
	) {
		return ofCollection(elementSerializer, collectionType, collectionType);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofCollection(
		SerializerDef elementSerializer, Class<? extends Collection> encodeType, Class<? extends Collection> decodeType
	) {
		return new RegularCollectionSerializerDef(elementSerializer, encodeType, decodeType, Object.class, false);
	}

	public static SerializerDef ofList(SerializerDef elementSerializer) {
		return new ListSerializerDef(elementSerializer, false);
	}

	public static SerializerDef ofLinkedList(SerializerDef elementSerializer) {
		return new LinkedListSerializerDef(elementSerializer, false);
	}

	public static SerializerDef ofSet(SerializerDef elementSerializer) {
		return new SetSerializerDef(elementSerializer, false);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofHashSet(
		SerializerDef elementSerializer, Class<? extends HashSet> encodeType, Class<? extends HashSet> decodeType
	) {
		return new HashSetSerializerDef(elementSerializer, encodeType, decodeType, false);
	}

	public static SerializerDef ofEnumSet(SerializerDef elementSerializer) {
		return new EnumSetSerializerDef(elementSerializer, false);
	}

	public static SerializerDef ofMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		return new MapSerializerDef(keySerializer, valueSerializer, false);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofHashMap(
		SerializerDef keySerializer, SerializerDef valueSerializer,
		Class<? extends HashMap> encodeType, Class<? extends HashMap> decodeType
	) {
		return new HashMapSerializerDef(keySerializer, valueSerializer, encodeType, decodeType, false);
	}

	public static SerializerDef ofEnumMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		return new EnumMapSerializerDef(keySerializer, valueSerializer, false);
	}

	public static SerializerDef ofByteBuffer(boolean wrapped) {
		return new ByteBufferSerializerDef(wrapped, false);
	}

	public static SerializerDef ofByteBuffer() {
		return new ByteBufferSerializerDef(false, false);
	}

	public static SerializerDef ofNullable(SerializerDef serializer) {
		return new NullableSerializerDef(serializer);
	}

	private static Class<?> getArrayType(Class<?> componentType) {
		Class<?> arrayClass;
		try {
			arrayClass = Utils.getArrayClass(componentType);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Could not obtain array class for: " + componentType, e);
		}
		return arrayClass;
	}
}
