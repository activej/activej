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
		return new ArrayDef(elementSerializer, -1, elementClass, false);
	}

	public static SerializerDef ofBoolean(boolean wrapped) {
		return new BooleanDef(wrapped, false);
	}

	public static SerializerDef ofChar(boolean wrapped) {
		return new CharDef(wrapped);
	}

	public static SerializerDef ofByte(boolean wrapped) {
		return new ByteDef(wrapped);
	}

	public static SerializerDef ofShort(boolean wrapped) {
		return new ShortDef(wrapped);
	}

	public static SerializerDef ofInt(boolean wrapped) {
		return new IntDef(wrapped, false);
	}

	public static SerializerDef ofInt(boolean wrapped, boolean varLength) {
		return new IntDef(wrapped, varLength);
	}

	public static SerializerDef ofLong(boolean wrapped) {
		return new LongDef(wrapped, false);
	}

	public static SerializerDef ofLong(boolean wrapped, boolean varLength) {
		return new LongDef(wrapped, varLength);
	}

	public static SerializerDef ofFloat(boolean wrapped) {
		return new FloatDef(wrapped);
	}

	public static SerializerDef ofDouble(boolean wrapped) {
		return new DoubleDef(wrapped);
	}

	public static SerializerDef ofString(StringFormat format) {
		return new StringDef(format, false);
	}

	public static <E extends Enum<E>> SerializerDef ofEnum(Class<E> enumClass) {
		return new EnumDef(enumClass, false);
	}

	public static SerializerDef ofInet4Address() {
		return new Inet4AddressDef();
	}

	public static SerializerDef ofInet6Address() {
		return new Inet6AddressDef();
	}

	public static SerializerDef ofInetAddress() {
		SubclassDef.Builder subclassBuilder = SubclassDef.builder(InetAddress.class);
		subclassBuilder.withSubclass(Inet4Address.class, new Inet4AddressDef());
		subclassBuilder.withSubclass(Inet6Address.class, new Inet6AddressDef());
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
		return new RegularCollectionDef(elementSerializer, encodeType, decodeType, Object.class, false);
	}

	public static SerializerDef ofList(SerializerDef elementSerializer) {
		return new ListDef(elementSerializer, false);
	}

	public static SerializerDef ofLinkedList(SerializerDef elementSerializer) {
		return new LinkedListDef(elementSerializer, false);
	}

	public static SerializerDef ofSet(SerializerDef elementSerializer) {
		return new SetDef(elementSerializer, false);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofHashSet(SerializerDef elementSerializer,
			Class<? extends HashSet> encodeType, Class<? extends HashSet> decodeType) {
		return new HashSetDef(elementSerializer, encodeType, decodeType, false);
	}

	public static SerializerDef ofEnumSet(SerializerDef elementSerializer) {
		return new EnumSetDef(elementSerializer, false);
	}

	public static SerializerDef ofMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		return new MapDef(keySerializer, valueSerializer, false);
	}

	@SuppressWarnings("rawtypes")
	public static SerializerDef ofHashMap(SerializerDef keySerializer, SerializerDef valueSerializer,
			Class<? extends HashMap> encodeType, Class<? extends HashMap> decodeType) {
		return new HashMapDef(keySerializer, valueSerializer, encodeType, decodeType, false);
	}

	public static SerializerDef ofEnumMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		return new EnumMapDef(keySerializer, valueSerializer, false);
	}

	public static SerializerDef ofByteBuffer(boolean wrapped) {
		return new ByteBufferDef(wrapped, false);
	}

	public static SerializerDef ofByteBuffer() {
		return new ByteBufferDef(false, false);
	}

	public static SerializerDef ofNullable(SerializerDef serializer) {
		return new NullableDef(serializer);
	}
}
