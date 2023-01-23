package io.activej.serializer;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.types.TypeT;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SerializePathsTest {

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static final BinarySerializer<TestData<String>> serializer = SerializerFactory.defaultInstance(DEFINING_CLASS_LOADER)
			.create(new TypeT<TestData<String>>() {}.getType());
	private static final BinarySerializer<TestDataWithPaths<String>> serializerPaths = SerializerFactory.builder(DEFINING_CLASS_LOADER)
			.withAnnotationCompatibilityMode()
			.build()
			.create(new TypeT<TestDataWithPaths<String>>() {}.getType());

	@Test
	public void testAllNull() {
		TestData<String> testData = new TestData<>();
		TestDataWithPaths<String> testDataWithPaths = new TestDataWithPaths<>();

		doTest(testData, testDataWithPaths);
	}

	@Test
	public void testString() {
		String[] array = new String[]{"a", "b", null, "c"};
		TestData<String> testData = new TestData<>();
		TestDataWithPaths<String> testDataWithPaths = new TestDataWithPaths<>();
		testData.arrayOfStrings = array;
		testDataWithPaths.arrayOfStrings = array;

		doTest(testData, testDataWithPaths);
	}

	@Test
	public void test() {
		TestData<String> testData = new TestData<>();
		TestDataWithPaths<String> testDataWithPaths = new TestDataWithPaths<>();

		String string = "test";
		testData.string = string;
		testDataWithPaths.string = string;

		String[] array = new String[]{"a", "b", null, "c"};
		testData.arrayOfStrings = array;
		testDataWithPaths.arrayOfStrings = array;

		List<Map<Integer, String>> listOfMaps = new ArrayList<>();
		Map<Integer, String> newMap1 = new HashMap<>();
		newMap1.put(1, "a");
		newMap1.put(2, null);
		newMap1.put(3, "c");
		listOfMaps.add(newMap1);
		listOfMaps.add(null);
		Map<Integer, String> newMap2 = new HashMap<>();
		newMap2.put(1, "a");
		newMap2.put(2, null);
		newMap2.put(null, "c");
		listOfMaps.add(newMap2);
		testData.listOfMaps = listOfMaps;
		testDataWithPaths.listOfMaps = listOfMaps;

		List<Set<? extends String>> listOfSets = new ArrayList<>();
		Set<String> set1 = new HashSet<>();
		set1.add("a");
		set1.add(null);
		listOfSets.add(set1);
		listOfSets.add(null);
		testData.listOfSets = listOfSets;
		testDataWithPaths.listOfSets = listOfSets;

		doTest(testData, testDataWithPaths);
	}

	@Test
	public void testArrayOfStrings() {
		String[] array = new String[]{"a", "b", null, "c"};
		TestData<String> testData = new TestData<>();
		TestDataWithPaths<String> testDataWithPaths = new TestDataWithPaths<>();
		testData.arrayOfStrings = array;
		testDataWithPaths.arrayOfStrings = array;

		doTest(testData, testDataWithPaths);
	}

	public static class TestData<T> {
		@Serialize
		@SerializeNullable
		public String string;

		@Serialize
		@SerializeNullable
		public Map<@SerializeNullable String, @SerializeNullable Integer> map;

		@Serialize
		@SerializeNullable
		public String @SerializeNullable [] arrayOfStrings;

		@Serialize
		@SerializeNullable
		public List<@SerializeNullable Map<@SerializeNullable Integer, @SerializeNullable T>> listOfMaps;

		@Serialize
		@SerializeNullable
		public List<@SerializeNullable Set<@SerializeNullable ? extends T>> listOfSets;
	}

	public static class TestDataWithPaths<T> {
		@Serialize
		@SerializeNullable
		public String string;

		@Serialize
		@SerializeNullable
		@SerializeNullable(path = 0)
		@SerializeNullable(path = 1)
		public Map<String, Integer> map;

		@Serialize
		@SerializeNullable
		@SerializeNullable(path = 0)
		public String[] arrayOfStrings;

		@Serialize
		@SerializeNullable
		@SerializeNullable(path = 0)
		@SerializeNullable(path = {0, 0})
		@SerializeNullable(path = {0, 1})
		public List<Map<Integer, T>> listOfMaps;

		@Serialize
		@SerializeNullable
		@SerializeNullable(path = 0)
		@SerializeNullable(path = {0, 0})
		public List<Set<? extends T>> listOfSets;
	}

	private static void doTest(TestData<String> testData, TestDataWithPaths<String> testDataWithPaths) {
		byte[] array1 = new byte[1000];
		serializer.encode(array1, 0, testData);
		TestData<String> decoded = serializer.decode(array1, 0);
		assertTestData(testData, decoded);

		byte[] array2 = new byte[1000];
		serializerPaths.encode(array2, 0, testDataWithPaths);
		TestDataWithPaths<String> decodedPaths = serializerPaths.decode(array2, 0);
		assertTestData(testDataWithPaths, decodedPaths);

		assertArrayEquals(array1, array2);
		assertTestData(decoded, decodedPaths);
	}

	private static void assertTestData(TestData<String> testData1, TestData<String> testData2) {
		assertTestData(
				testData1.string, testData2.string,
				testData1.map, testData2.map,
				testData1.arrayOfStrings, testData2.arrayOfStrings,
				testData1.listOfMaps, testData2.listOfMaps,
				testData1.listOfSets, testData2.listOfSets
		);
	}

	private static void assertTestData(TestDataWithPaths<String> testData1, TestDataWithPaths<String> testData2) {
		assertTestData(
				testData1.string, testData2.string,
				testData1.map, testData2.map,
				testData1.arrayOfStrings, testData2.arrayOfStrings,
				testData1.listOfMaps, testData2.listOfMaps,
				testData1.listOfSets, testData2.listOfSets
		);
	}

	private static void assertTestData(TestData<String> testData1, TestDataWithPaths<String> testData2) {
		assertTestData(
				testData1.string, testData2.string,
				testData1.map, testData2.map,
				testData1.arrayOfStrings, testData2.arrayOfStrings,
				testData1.listOfMaps, testData2.listOfMaps,
				testData1.listOfSets, testData2.listOfSets
		);
	}

	private static void assertTestData(
			String string1, String string2,
			Map<String, Integer> map1, Map<String, Integer> map2,
			String[] arrayOfStrings1, String[] arrayOfStrings2,
			List<Map<Integer, String>> listOfMaps1, List<Map<Integer, String>> listOfMaps2,
			List<Set<? extends String>> listOfSets1, List<Set<? extends String>> listOfSets2) {
		assertEquals(string1, string2);
		assertEquals(map1, map2);
		assertArrayEquals(arrayOfStrings1, arrayOfStrings2);
		assertEquals(listOfMaps1, listOfMaps2);
		assertEquals(listOfSets1, listOfSets2);
	}
}
