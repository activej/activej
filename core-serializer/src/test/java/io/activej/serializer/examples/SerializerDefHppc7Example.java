package io.activej.serializer.examples;

import com.carrotsearch.hppc.*;
import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static io.activej.serializer.Utils.doTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class SerializerDefHppc7Example {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static <T> BinarySerializer<T> getBufferSerializer(Class<T> collectionType) {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		return SerializerFactoryUtils.createWithHppc7Support(classLoader)
				.create(classLoader, collectionType);
	}

	@Test
	public void testIntByteMap() {
		BinarySerializer<IntByteMap> serializer = getBufferSerializer(IntByteMap.class);

		IntByteMap testMap1 = new IntByteHashMap();

		IntByteMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, (byte) 10);
		testMap1.put(1, (byte) 11);
		IntByteMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testIntSet() {
		BinarySerializer<IntSet> serializer = getBufferSerializer(IntSet.class);

		IntSet test1 = new IntHashSet();
		IntSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add((byte) 10);
		test1.add((byte) 11);

		IntSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testIntArrayList() {
		BinarySerializer<IntArrayList> serializer = getBufferSerializer(IntArrayList.class);

		IntArrayList test1 = new IntArrayList();
		IntArrayList test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add(10);
		test1.add(11);

		IntArrayList test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}
}

