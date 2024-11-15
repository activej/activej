package hppc;

import com.carrotsearch.hppc.*;
import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;

public final class Hppc9SerializerDefExample {

	public static void main(String[] args) {
		serializeIntByteMap();
		serializeIntSet();
		serializeIntArrayList();
	}

	private static void serializeIntByteMap() {
		BinarySerializer<IntByteMap> serializer = createBufferSerializer(IntByteMap.class);

		IntByteMap testMap1 = new IntByteHashMap();

		IntByteMap testMap2 = serializeAndDeserialize(testMap1, serializer);
		System.out.println(testMap1);
		System.out.println(testMap2);
		System.out.println();

		testMap1.put(0, (byte) 10);
		testMap1.put(1, (byte) 11);

		IntByteMap testMap3 = serializeAndDeserialize(testMap1, serializer);
		System.out.println(testMap1);
		System.out.println(testMap3);
		System.out.println();
	}

	private static void serializeIntSet() {
		BinarySerializer<IntSet> serializer = createBufferSerializer(IntSet.class);

		IntSet testSet1 = new IntHashSet();
		IntSet testSet2 = serializeAndDeserialize(testSet1, serializer);
		System.out.println(testSet1);
		System.out.println(testSet2);
		System.out.println();

		testSet1.add((byte) 10);
		testSet1.add((byte) 11);

		IntSet testSet3 = serializeAndDeserialize(testSet1, serializer);
		System.out.println(testSet1);
		System.out.println(testSet3);
		System.out.println();
	}

	private static void serializeIntArrayList() {
		BinarySerializer<IntArrayList> serializer = createBufferSerializer(IntArrayList.class);

		IntArrayList testList1 = new IntArrayList();
		IntArrayList testList2 = serializeAndDeserialize(testList1, serializer);
		System.out.println(testList1);
		System.out.println(testList2);
		System.out.println();

		testList1.add(10);
		testList1.add(11);

		IntArrayList testList3 = serializeAndDeserialize(testList1, serializer);
		System.out.println(testList1);
		System.out.println(testList3);
		System.out.println();
	}

	private static <T> T serializeAndDeserialize(T testData, BinarySerializer<T> serializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData);
		return serializer.decode(array, 0);
	}

	private static <T> BinarySerializer<T> createBufferSerializer(Class<T> collectionType) {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		return SerializerFactoryUtils.createWithHppc7Support(classLoader)
			.create(classLoader, collectionType);
	}
}

