package io.activej.serializer.def;

import io.activej.common.annotation.StaticFactories;
import io.activej.serializer.StringFormat;
import io.activej.serializer.def.impl.*;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import static io.activej.common.Checks.checkArgument;

@StaticFactories(SerializerDef.class)
public class SerializerDefs {

	public static SerializerDef ofArray(SerializerDef elementSerializer) {
		checkArgument(elementSerializer.getEncodeType() == elementSerializer.getDecodeType(),
				"Ambiguous encode and decode types");
		return ofArray(elementSerializer, elementSerializer.getEncodeType());
	}

	public static SerializerDef ofArray(SerializerDef elementSerializer, Class<?> elementClass) {
		return new ArraySerializer(elementSerializer, -1, elementClass, false);
	}

	public static SerializerDef ofBoolean(boolean wrapped) {
		return new BooleanSerializer(wrapped, false);
	}

	public static SerializerDef ofChar(boolean wrapped) {
		return new CharSerializer(wrapped);
	}

	public static SerializerDef ofByte(boolean wrapped) {
		return new ByteSerializer(wrapped);
	}

	public static SerializerDef ofShort(boolean wrapped) {
		return new ShortSerializer(wrapped);
	}

	public static SerializerDef ofInt(boolean wrapped) {
		return new IntSerializer(wrapped, false);
	}

	public static SerializerDef ofInt(boolean wrapped, boolean varLength) {
		return new IntSerializer(wrapped, varLength);
	}

	public static SerializerDef ofLong(boolean wrapped) {
		return new LongSerializer(wrapped, false);
	}

	public static SerializerDef ofLong(boolean wrapped, boolean varLength) {
		return new LongSerializer(wrapped, varLength);
	}

	public static SerializerDef ofFloat(boolean wrapped) {
		return new FloatSerializer(wrapped);
	}

	public static SerializerDef ofDouble(boolean wrapped) {
		return new DoubleSerializer(wrapped);
	}

	public static SerializerDef ofString(StringFormat format) {
		return new StringSerializer(format, false);
	}

	public static <E extends Enum<E>> SerializerDef ofEnum(Class<E> enumClass) {
		return new EnumSerializer(enumClass, false);
	}

	public static SerializerDef ofInet4Address() {
		return new Inet4AddressSerializer();
	}

	public static SerializerDef ofInet6Address() {
		return new Inet6AddressSerializer();
	}

	public static SerializerDef ofInetAddress() {
		SubclassSerializer.Builder subclassBuilder = SubclassSerializer.builder(InetAddress.class);
		subclassBuilder.withSubclass(Inet4Address.class, new Inet4AddressSerializer());
		subclassBuilder.withSubclass(Inet6Address.class, new Inet6AddressSerializer());
		return subclassBuilder.build();
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofCollection(SerializerDef elementSerializer,
			Class<? extends Collection> collectionType) {
		return ofCollection(elementSerializer, collectionType, collectionType);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofCollection(SerializerDef elementSerializer,
			Class<? extends Collection> encodeType, Class<? extends Collection> decodeType) {
		return new RegularCollectionSerializer(elementSerializer, encodeType, decodeType, Object.class, false);
	}

	public static SerializerDef ofList(SerializerDef elementSerializer) {
		return new ListSerializer(elementSerializer, false);
	}

	public static SerializerDef ofLinkedList(SerializerDef elementSerializer) {
		return new LinkedListSerializer(elementSerializer, false);
	}

	public static SerializerDef ofSet(SerializerDef elementSerializer) {
		return new SetSerializer(elementSerializer, false);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofHashSet(SerializerDef elementSerializer,
			Class<? extends HashSet> encodeType, Class<? extends HashSet> decodeType) {
		return new HashSetSerializer(elementSerializer, encodeType, decodeType, false);
	}

	public static SerializerDef ofEnumSet(SerializerDef elementSerializer) {
		return new EnumSetSerializer(elementSerializer, false);
	}

	public static SerializerDef ofMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		return new MapSerializer(keySerializer, valueSerializer, false);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofHashMap(SerializerDef keySerializer, SerializerDef valueSerializer,
			Class<? extends HashMap> encodeType, Class<? extends HashMap> decodeType) {
		return new HashMapSerializer(keySerializer, valueSerializer, encodeType, decodeType, false);
	}

	public static SerializerDef ofEnumMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		return new EnumMapSerializer(keySerializer, valueSerializer, false);
	}

	public static SerializerDef ofByteBuffer(boolean wrapped) {
		return new ByteBufferSerializer(wrapped, false);
	}

	public static SerializerDef ofByteBuffer() {
		return new ByteBufferSerializer(false, false);
	}

	public static SerializerDef ofNullable(SerializerDef serializer) {
		return new NullableSerializer(serializer);
	}
}
