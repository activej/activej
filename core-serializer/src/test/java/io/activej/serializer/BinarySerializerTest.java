package io.activej.serializer;

import io.activej.serializer.annotations.*;
import io.activej.serializer.impl.*;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.serializer.StringFormat.*;
import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static io.activej.serializer.Utils.doTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

@SuppressWarnings({"unused", "ArraysAsListWithZeroOrOneArgument"})
public class BinarySerializerTest {

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	public static class TestDataScalars {
		public enum TestEnum {
			ONE(1), TWO(2), THREE(3);

			TestEnum(@SuppressWarnings("UnusedParameters") int id) {
			}
		}

		@Serialize
		public boolean z;
		@Serialize
		public char c;
		@Serialize
		public byte b;
		@Serialize
		public short s;
		@Serialize
		public int i;
		@Serialize
		public long l;
		@Serialize
		public float f;
		@Serialize
		public double d;
		@SerializeVarLength
		@Serialize
		public int iVar;
		@SerializeVarLength
		@Serialize
		public long lVar;

		@Serialize
		public Boolean zBoxed;
		@Serialize
		public Character cBoxed;
		@Serialize
		public Byte bBoxed;
		@Serialize
		public Short sBoxed;
		@Serialize
		public Integer iBoxed;
		@Serialize
		public Long lBoxed;
		@Serialize
		public Float fBoxed;
		@Serialize
		public Double dBoxed;
		@SerializeVarLength
		@Serialize
		public int iBoxedVar;
		@SerializeVarLength
		@Serialize
		public long lBoxedVar;

		@Serialize
		public byte[] bytes;

		@Serialize
		public String string;
		@Serialize
		public TestEnum testEnum;
		@Serialize
		public InetAddress address;
	}

	@Test
	public void testScalars() throws UnknownHostException {
		TestDataScalars testData1 = new TestDataScalars();

		Random rnd = new Random();

		testData1.z = rnd.nextBoolean();
		testData1.c = (char) rnd.nextInt(Character.MAX_VALUE);
		testData1.b = (byte) rnd.nextInt(1 << 8);
		testData1.s = (byte) rnd.nextInt(1 << 16);
		testData1.i = rnd.nextInt();
		testData1.l = rnd.nextLong();
		testData1.f = rnd.nextFloat();
		testData1.d = rnd.nextDouble();
		testData1.iVar = rnd.nextInt();
		testData1.lVar = rnd.nextLong();

		testData1.zBoxed = true;
		testData1.cBoxed = Character.MAX_VALUE;
		testData1.bBoxed = Byte.MIN_VALUE;
		testData1.sBoxed = Short.MIN_VALUE;
		testData1.iBoxed = Integer.MIN_VALUE;
		testData1.lBoxed = Long.MIN_VALUE;
		testData1.fBoxed = Float.MIN_VALUE;
		testData1.dBoxed = Double.MIN_VALUE;
		testData1.iBoxedVar = Integer.MIN_VALUE;
		testData1.lBoxedVar = Long.MIN_VALUE;

		testData1.bytes = new byte[]{1, 2, 3};
		testData1.string = "abc";
		testData1.testEnum = TestDataScalars.TestEnum.TWO;
		testData1.address = InetAddress.getByName("127.0.0.1");

		TestDataScalars testData2 = doTest(TestDataScalars.class, testData1);

		assertEquals(testData1.z, testData2.z);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.b, testData2.b);
		assertEquals(testData1.s, testData2.s);
		assertEquals(testData1.i, testData2.i);
		assertEquals(testData1.l, testData2.l);
		assertEquals(testData1.f, testData2.f, Double.MIN_VALUE);
		assertEquals(testData1.d, testData2.d, Double.MIN_VALUE);
		assertEquals(testData1.iVar, testData2.iVar);
		assertEquals(testData1.lVar, testData2.lVar);

		assertEquals(testData1.zBoxed, testData2.zBoxed);
		assertEquals(testData1.cBoxed, testData2.cBoxed);
		assertEquals(testData1.bBoxed, testData2.bBoxed);
		assertEquals(testData1.sBoxed, testData2.sBoxed);
		assertEquals(testData1.iBoxed, testData2.iBoxed);
		assertEquals(testData1.lBoxed, testData2.lBoxed);
		assertEquals(testData1.fBoxed, testData2.fBoxed);
		assertEquals(testData1.dBoxed, testData2.dBoxed);
		assertEquals(testData1.iBoxedVar, testData2.iBoxedVar);
		assertEquals(testData1.lBoxedVar, testData2.lBoxedVar);

		assertArrayEquals(testData1.bytes, testData2.bytes);
		assertEquals(testData1.string, testData2.string);
		assertEquals(testData1.testEnum, testData2.testEnum);
		assertEquals(testData1.address, testData2.address);
	}

	public static class TestDataDeserialize {
		public TestDataDeserialize(@Deserialize("finalInt") int finalInt, @Deserialize("finalString") String finalString) {
			this.finalInt = finalInt;
			this.finalString = finalString;
		}

		@Serialize
		public final int finalInt;

		@Serialize
		public final String finalString;

		private int i;
		private int iBoxed;

		private int getterInt;
		private String getterString;

		@Serialize
		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		@Serialize
		public int getIBoxed() {
			return iBoxed;
		}

		public void setIBoxed(int iBoxed) {
			this.iBoxed = iBoxed;
		}

		@Serialize
		public int getGetterInt() {
			return getterInt;
		}

		@Serialize
		public String getGetterString() {
			return getterString;
		}

		public void setMultiple(@Deserialize("getterInt") int getterInt, @Deserialize("getterString") String getterString) {
			this.getterInt = getterInt;
			this.getterString = getterString;
		}
	}

	@Test
	public void testDeserialize() {
		TestDataDeserialize testData1 = new TestDataDeserialize(10, "abc");
		testData1.setI(20);
		testData1.setIBoxed(30);
		testData1.setMultiple(40, "123");

		TestDataDeserialize testData2 = doTest(TestDataDeserialize.class, testData1);
		assertEquals(testData1.finalInt, testData2.finalInt);
		assertEquals(testData1.finalString, testData2.finalString);
		assertEquals(testData1.getI(), testData2.getI());
		assertEquals(testData1.getIBoxed(), testData2.getIBoxed());
		assertEquals(testData1.getGetterInt(), testData2.getGetterInt());
		assertEquals(testData1.getGetterString(), testData2.getGetterString());
	}

	public static class TestDataNested {
		private final int a;

		private int b;

		public TestDataNested(@Deserialize("a") int a) {
			this.a = a;
		}

		@Serialize
		public int getA() {
			return a;
		}

		@Serialize
		public int getB() {
			return b;
		}

		public void setB(int b) {
			this.b = b;
		}
	}

	public static class TestDataComplex {
		@Serialize
		public TestDataNested nested;
		@Serialize
		public TestDataNested[] nestedArray;
		@Serialize
		public TestDataNested[][] nestedArrayArray;
		@Serialize
		public List<TestDataNested> nestedList;
		@Serialize
		public List<List<TestDataNested>> nestedListList;

		@Serialize
		public int[] ints;

		@Serialize
		public int[][] intsArray;
	}

	private void assertEqualNested(TestDataNested testDataNested1, TestDataNested testDataNested2) {
		assertEquals(testDataNested1.getA(), testDataNested2.getA());
		assertEquals(testDataNested1.getB(), testDataNested2.getB());
	}

	@Test
	public void testComplex() {
		TestDataComplex testData1 = new TestDataComplex();

		testData1.ints = new int[]{1, 2, 3};
		testData1.intsArray = new int[][]{
				new int[]{1, 2},
				new int[]{3, 4, 5}};

		testData1.nested = new TestDataNested(11);
		testData1.nestedArray = new TestDataNested[]{new TestDataNested(12), new TestDataNested(13)};
		testData1.nestedArrayArray = new TestDataNested[][]{
				new TestDataNested[]{new TestDataNested(14), new TestDataNested(15)},
				new TestDataNested[]{new TestDataNested(16)}};
		testData1.nestedList = asList(new TestDataNested(1), new TestDataNested(2));
		testData1.nestedListList = asList(
				asList(new TestDataNested(20), new TestDataNested(21)),
				singletonList(new TestDataNested(22)));

		TestDataComplex testData2 = doTest(TestDataComplex.class, testData1);

		assertArrayEquals(testData1.ints, testData2.ints);

		assertEquals(testData1.intsArray.length, testData2.intsArray.length);
		for (int i = 0; i < testData1.intsArray.length; i++) {
			assertArrayEquals(testData1.intsArray[i], testData2.intsArray[i]);
		}

		assertEqualNested(testData1.nested, testData2.nested);

		assertEquals(testData1.nestedArray.length, testData2.nestedArray.length);
		for (int i = 0; i < testData1.nestedArray.length; i++) {
			assertEqualNested(testData1.nestedArray[i], testData2.nestedArray[i]);
		}

		assertEquals(testData1.nestedArrayArray.length, testData2.nestedArrayArray.length);
		for (int i = 0; i < testData1.nestedArrayArray.length; i++) {
			TestDataNested[] nested1 = testData1.nestedArrayArray[i];
			TestDataNested[] nested2 = testData2.nestedArrayArray[i];
			assertEquals(nested1.length, nested2.length);
			for (int j = 0; j < nested1.length; j++) {
				assertEqualNested(nested1[j], nested2[j]);
			}
		}

		assertEquals(testData1.nestedList.size(), testData2.nestedList.size());
		for (int i = 0; i < testData1.nestedList.size(); i++) {
			assertEqualNested(testData1.nestedList.get(i), testData2.nestedList.get(i));
		}

		assertEquals(testData1.nestedListList.size(), testData2.nestedListList.size());
		for (int i = 0; i < testData1.nestedListList.size(); i++) {
			List<TestDataNested> nested1 = testData1.nestedListList.get(i);
			List<TestDataNested> nested2 = testData2.nestedListList.get(i);
			assertEquals(nested1.size(), nested2.size());
			for (int j = 0; j < nested1.size(); j++) {
				assertEqualNested(nested1.get(j), nested2.get(j));
			}
		}
	}

	public static class TestDataNullables {
		@Serialize
		@SerializeNullable
		public String nullableString1;

		@Serialize
		@SerializeNullable
		public String nullableString2;

		@Serialize
		public List<@SerializeNullable String> listOfNullableStrings;

		@Serialize
		@SerializeNullable
		public String @SerializeNullable [] @SerializeNullable [] nullableArrayOfNullableArrayOfNullableStrings;

		@Serialize
		public Map<@SerializeNullable Integer, @SerializeNullable String> mapOfNullableInt2NullableString;
	}

	@Test
	public void testNullables() {
		TestDataNullables testData1 = new TestDataNullables();

		testData1.nullableString1 = null;
		testData1.nullableString2 = "abc";
		testData1.listOfNullableStrings = asList("a", null, "b");
		testData1.nullableArrayOfNullableArrayOfNullableStrings = new String[][]{
				new String[]{"a", null},
				null};
		testData1.mapOfNullableInt2NullableString = new LinkedHashMap<>();
		testData1.mapOfNullableInt2NullableString.put(1, "abc");
		testData1.mapOfNullableInt2NullableString.put(2, null);
		testData1.mapOfNullableInt2NullableString.put(null, "xyz");

		TestDataNullables testData2 = doTest(TestDataNullables.class, testData1);

		assertEquals(testData1.nullableString1, testData2.nullableString1);
		assertEquals(testData1.nullableString2, testData2.nullableString2);

		assertEquals(testData1.listOfNullableStrings, testData2.listOfNullableStrings);

		assertEquals(
				testData1.nullableArrayOfNullableArrayOfNullableStrings.length,
				testData2.nullableArrayOfNullableArrayOfNullableStrings.length);
		for (int i = 0; i < testData1.nullableArrayOfNullableArrayOfNullableStrings.length; i++) {
			assertArrayEquals(
					testData1.nullableArrayOfNullableArrayOfNullableStrings[i],
					testData2.nullableArrayOfNullableArrayOfNullableStrings[i]);
		}

		assertEquals(testData1.mapOfNullableInt2NullableString, testData2.mapOfNullableInt2NullableString);
	}

	public interface TestDataInterface {
		@Serialize
		int getI();

		@Serialize
		Integer getIBoxed();
	}

	public static class TestDataInterfaceImpl implements TestDataInterface {
		private int i;
		private Integer iBoxed;

		@Override
		public int getI() {
			return i;
		}

		@Override
		public Integer getIBoxed() {
			return iBoxed;
		}
	}

	@Test
	public void testInterface() {
		TestDataInterfaceImpl testData1 = new TestDataInterfaceImpl();
		testData1.i = 10;
		testData1.iBoxed = 20;

		TestDataInterface testData2 = doTest(TestDataInterface.class, testData1);
		assertEquals(testData1.getI(), testData2.getI());
		assertEquals(testData1.getIBoxed(), testData2.getIBoxed());
	}

	public static class ListOfStringHolder {
		@Serialize
		public List<String> list;
	}

	@Test
	public void testList() {
		ListOfStringHolder testData1 = new ListOfStringHolder();
		testData1.list = asList("a", "b", "c");
		ListOfStringHolder testData2 = doTest(ListOfStringHolder.class, testData1);
		assertEquals(testData1.list, testData2.list);
	}

	public static class MapIntegerStringHolder {
		@Serialize
		public Map<Integer, String> map;
	}

	@Test
	public void testMap() {
		MapIntegerStringHolder testData1 = new MapIntegerStringHolder();
		testData1.map = new HashMap<>();
		testData1.map.put(1, "abc");
		testData1.map.put(2, "xyz");
		MapIntegerStringHolder testData2 = doTest(MapIntegerStringHolder.class, testData1);
		assertEquals(testData1.map, testData2.map);
	}

	public interface TestDataGenericNestedInterface<K, V> {
		@Serialize
		K getKey();

		@Serialize
		V getValue();
	}

	public static class TestDataGenericNested<K, V> implements TestDataGenericNestedInterface<K, V> {
		private K key;

		private V value;

		public TestDataGenericNested() {
		}

		public TestDataGenericNested(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Serialize
		@Override
		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		@Serialize
		@Override
		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}
	}

	public interface TestDataGenericInterface<K, V> {
		@Serialize
		List<TestDataGenericNested<K, V>> getList();
	}

	public static class TestDataGeneric<K, V> implements TestDataGenericInterface<K, V> {
		private List<TestDataGenericNested<K, V>> list;

		@Serialize
		@Override
		public List<TestDataGenericNested<K, V>> getList() {
			return list;
		}

		public void setList(List<TestDataGenericNested<K, V>> list) {
			this.list = list;
		}
	}

	private static void assertEqualsGenericNested(TestDataGenericNested<Integer, String> item1, TestDataGenericNested<Integer, String> item2) {
		if (item1 != null || item2 != null) {
			assertNotNull(item1);
			assertNotNull(item2);
			assertEquals(item1.getKey(), item2.getKey());
			assertEquals(item1.getValue(), item2.getValue());
		}
	}

	public static class GenericHolder {
		@Serialize
		public TestDataGeneric<Integer, String> data;
	}

	@Test
	public void testGeneric() {
		GenericHolder testData1 = new GenericHolder();
		testData1.data = new TestDataGeneric<>();
		testData1.data.setList(asList(
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(20, "b")
		));
		GenericHolder testData2 = doTest(GenericHolder.class, testData1);
		assertEquals(testData1.data.list.size(), testData2.data.list.size());
		for (int i = 0; i < testData1.data.list.size(); i++) {
			assertEqualsGenericNested(testData1.data.list.get(i), testData2.data.list.get(i));
		}
	}

	public static class TestDataGenericParameters {
		@Serialize
		public List<@SerializeNullable TestDataGenericNested<@SerializeVarLength @SerializeNullable Integer, @SerializeStringFormat(UTF16) @SerializeNullable String>> list;
	}

	@Test
	public void testGenericParameters() {
		TestDataGenericParameters testData1 = new TestDataGenericParameters();
		testData1.list = asList(
				null,
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(null, null));
		TestDataGenericParameters testData2 = doTest(TestDataGenericParameters.class, testData1);
		assertEquals(testData1.list.size(), testData2.list.size());
		for (int i = 0; i < testData1.list.size(); i++) {
			assertEqualsGenericNested(testData1.list.get(i), testData2.list.get(i));
		}
	}

	public static class TestDataGenericInterfaceHolder {
		@Serialize
		public TestDataGenericInterface<Integer, String> data;
	}

	@Test
	public void testGenericInterface() {
		TestDataGenericInterfaceHolder testData1 = new TestDataGenericInterfaceHolder();
		TestDataGeneric<Integer, String> generic = new TestDataGeneric<>();
		generic.setList(asList(
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(20, "b")));

		testData1.data = generic;
		TestDataGenericInterfaceHolder testData2 = doTest(TestDataGenericInterfaceHolder.class, testData1);
		assertEquals(testData1.data.getList().size(), testData2.data.getList().size());
		for (int i = 0; i < testData1.data.getList().size(); i++) {
			assertEqualsGenericNested(testData1.data.getList().get(i), testData2.data.getList().get(i));
		}
	}

	public static class GenericNestedHolder {
		@Serialize
		public TestDataGenericNested<String, Integer> data;
	}

	@Test
	public void testGenericNested() {
		GenericNestedHolder testData1 = new GenericNestedHolder();
		testData1.data = new TestDataGenericNested<>("a", 10);
		GenericNestedHolder testData2 = doTest(GenericNestedHolder.class, testData1);
		assertEquals(testData1.data.key, testData2.data.key);
		assertEquals(testData1.data.value, testData2.data.value);
	}

	public static class GenericNestedInterfaceHolder {
		@Serialize
		public TestDataGenericNestedInterface<String, Integer> data;
	}

	@Test
	public void testGenericNestedInterface() {
		GenericNestedInterfaceHolder testData1 = new GenericNestedInterfaceHolder();
		testData1.data = new TestDataGenericNested<>("a", 10);
		GenericNestedInterfaceHolder testData2 = doTest(GenericNestedInterfaceHolder.class, testData1);
		assertEquals(testData1.data.getKey(), testData2.data.getKey());
		assertEquals(testData1.data.getValue(), testData2.data.getValue());
	}

	public static class TestDataGenericSuperclass<A, B> {
		@Serialize
		public A a;

		@Serialize
		public B b;
	}

	public static class TestDataGenericSubclass<X, Y> extends TestDataGenericSuperclass<Integer, X> {
		@Serialize
		public Y c;
	}

	public static class GenericSubclassHolder {
		@Serialize
		public TestDataGenericSubclass<String, Boolean> data;
	}

	@Test
	public void testGenericSubclass() {
		GenericSubclassHolder testData1 = new GenericSubclassHolder();
		testData1.data = new TestDataGenericSubclass<>();
		testData1.data.a = 10;
		testData1.data.b = "abc";
		testData1.data.c = true;
		GenericSubclassHolder testData2 = doTest(GenericSubclassHolder.class, testData1);
		assertEquals(testData1.data.a, testData2.data.a);
		assertEquals(testData1.data.b, testData2.data.b);
		assertEquals(testData1.data.c, testData2.data.c);
	}

	public static class TestDataSuperclassHolder {
		@Serialize
		@SerializeSubclasses({TestDataSubclass1.class, TestDataSubclass2.class})
		@SerializeNullable
		public TestDataSuperclass data;
	}

	public static class TestDataSuperclass {
		public TestDataSuperclass() {
			initByCons = 123;
		}

		@Serialize
		public int a;

		public int initByCons;
	}

	public static class TestDataSubclass1 extends TestDataSuperclass {
		@Serialize
		public boolean b;
	}

	public static class TestDataSubclass2 extends TestDataSuperclass {
		@Serialize
		@SerializeNullable
		public String s;
	}

	@Test
	public void testSubclasses1() {
		TestDataSuperclassHolder testData1 = new TestDataSuperclassHolder();
		testData1.data = null;
		TestDataSuperclassHolder testData2;
		testData2 = doTest(TestDataSuperclassHolder.class, testData1);
		assertEquals(testData1.data, testData2.data);

		TestDataSubclass1 subclass1 = new TestDataSubclass1();
		testData1.data = subclass1;
		subclass1.a = 10;
		subclass1.b = true;
		testData2 = doTest(TestDataSuperclassHolder.class, testData1);
		TestDataSubclass1 subclass2 = (TestDataSubclass1) testData2.data;
		assertEquals(subclass1.a, subclass2.a);
		assertEquals(subclass1.b, subclass2.b);
		assertEquals(subclass1.initByCons, subclass2.initByCons);
	}

	@Test
	public void testSubclasses2() {
		TestDataSuperclassHolder testData1 = new TestDataSuperclassHolder();
		TestDataSubclass2 subclass1 = new TestDataSubclass2();
		testData1.data = subclass1;
		subclass1.a = 10;
		subclass1.s = "abc";
		TestDataSuperclassHolder testData2 = doTest(TestDataSuperclassHolder.class, testData1);
		TestDataSubclass2 subclass2 = (TestDataSubclass2) testData2.data;
		assertEquals(subclass1.a, subclass2.a);
		assertEquals(subclass1.s, subclass2.s);

		subclass1.s = null;
		testData2 = doTest(TestDataSuperclassHolder.class, testData1);
		subclass2 = (TestDataSubclass2) testData2.data;
		assertEquals(subclass1.a, subclass2.a);
		assertEquals(subclass1.s, subclass2.s);
		assertEquals(subclass1.initByCons, subclass2.initByCons);
	}

	public static class TestDataSerializerFormat {
		@Serialize
		public List<@SerializeStringFormat(UTF16) @SerializeNullable String> stringsUtf16;

		@Serialize
		public List<@SerializeStringFormat(UTF8) @SerializeNullable String> stringsUtf8;

		@Serialize
		@SuppressWarnings("deprecation")
		public List<@SerializeStringFormat(UTF8_MB3) @SerializeNullable String> stringsUtf8Custom;

		@Serialize
		public List<@SerializeStringFormat(ISO_8859_1) @SerializeNullable String> stringsIso88591;

	}

	@Test
	public void testSerializerStringFormat() {
		TestDataSerializerFormat testData1 = new TestDataSerializerFormat();
		testData1.stringsUtf16 = asList("Abc-äöß-Абв-あア-😀", null, "123");
		testData1.stringsUtf8 = asList("Abc-äöß-Абв-あア-😀", null, "123");
		testData1.stringsUtf8Custom = asList("Abc-äöß-Абв-あア-😀", null, "123");
		testData1.stringsIso88591 = asList("Abc-äöß", null, "123");

		TestDataSerializerFormat testData2 = doTest(TestDataSerializerFormat.class, testData1);

		assertEquals(testData1.stringsUtf16, testData2.stringsUtf16);
		assertEquals(testData1.stringsUtf8, testData2.stringsUtf8);
		assertEquals(testData1.stringsUtf8Custom, testData2.stringsUtf8Custom);
		assertEquals(testData1.stringsIso88591, testData2.stringsIso88591);
	}

	public static class TestDataFixedSize {
		@Serialize
		public @SerializeNullable String @SerializeFixedSize(3) [] strings;

		@Serialize
		public byte @SerializeFixedSize(4) [] bytes;
	}

	@Test
	public void testFixedSize() {
		TestDataFixedSize testData1 = new TestDataFixedSize();
		testData1.strings = new String[]{"abc", null, "123", "superfluous"};
		testData1.bytes = new byte[]{1, 2, 3, 4};
		TestDataFixedSize testData2 = doTest(TestDataFixedSize.class, testData1);
		assertArrayEquals(new String[]{testData1.strings[0], testData1.strings[1], testData1.strings[2]}, testData2.strings);
		assertArrayEquals(testData1.bytes, testData2.bytes);
	}

	public static class TestDataVersions {
		@Serialize(added = 0)
		public int a;

		@Serialize(added = 1, removed = 2)
		public int b;

		@Serialize(added = 2)
		public int c;

		@Serialize(added = 10)
		public int d;
	}

	@Test
	public void testVersions() {
		BinarySerializer<TestDataVersions> serializer0 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(0).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer1 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(1).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer11 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withVersions(1, 1, 1).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer2 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(2).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer22 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withVersions(2, 2, 2).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer5 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(5).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer10 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withVersions(10, 10, 10).build(TestDataVersions.class);
		BinarySerializer<TestDataVersions> serializer100 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(100).build(TestDataVersions.class);

		TestDataVersions testData1 = new TestDataVersions();
		testData1.a = 10;
		testData1.b = 20;
		testData1.c = 30;

		TestDataVersions testData2;

		testData2 = doTest(testData1, serializer0, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer0, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer0, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData2 = doTest(testData1, serializer11, serializer11);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		try {
			doTest(testData1, serializer0, serializer11);
			fail();
		} catch (Exception ignored) {
		}

		testData2 = doTest(testData1, serializer2, serializer22);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData2 = doTest(testData1, serializer22, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData1.d = 100;
		testData2 = doTest(testData1, serializer5, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(0, testData2.d);

		testData2 = doTest(testData1, serializer100, serializer10);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.d, testData2.d);
	}

	public static class TestDataProfiles {
		@Serialize
		public int a;

		@Serialize
		@SerializeProfiles("profile1")
		public int b;

		@Serialize
		@SerializeProfiles({"profile1", "profile2"})
		public int c;
	}

	@Test
	public void testProfiles() {
		BinarySerializer<TestDataProfiles> serializer0 = SerializerBuilder.create(DEFINING_CLASS_LOADER).build(TestDataProfiles.class);
		BinarySerializer<TestDataProfiles> serializer1 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withProfile("profile1").build(TestDataProfiles.class);
		BinarySerializer<TestDataProfiles> serializer2 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withProfile("profile2").build(TestDataProfiles.class);

		TestDataProfiles testData1 = new TestDataProfiles();
		testData1.a = 10;
		testData1.b = 20;
		testData1.c = 30;

		TestDataProfiles testData2;

		testData2 = doTest(testData1, serializer0, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(testData2.c, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
	}

	public static class TestDataProfiles2 {
		@Serialize(added = 1)
		public int a;

		@Serialize(added = 1)
		@SerializeProfiles(value = "profile", added = 2)
		public int b;

		@SerializeProfiles(value = {"profile", SerializeProfiles.COMMON_PROFILE}, added = 1, removed = 2)
		@Serialize(added = 2)
		public int c;

		@Serialize(added = 2)
		public int d;

		@Serialize(added = 1)
		@SerializeProfiles("profile")
		public int e;

		public int f;
	}

	@Test
	public void testProfilesVersions() {
		Class<TestDataProfiles2> type = TestDataProfiles2.class;
		BinarySerializer<TestDataProfiles2> serializer1 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(1).build(type);
		BinarySerializer<TestDataProfiles2> serializer2 = SerializerBuilder.create(DEFINING_CLASS_LOADER).withEncodeVersion(2).build(type);

		BinarySerializer<TestDataProfiles2> serializer1Profile = SerializerBuilder.create(DEFINING_CLASS_LOADER).withProfile("profile").withEncodeVersion(1).build(type);
		BinarySerializer<TestDataProfiles2> serializer2Profile = SerializerBuilder.create(DEFINING_CLASS_LOADER).withProfile("profile").withEncodeVersion(2).build(type);

		TestDataProfiles2 testData1 = new TestDataProfiles2();
		testData1.a = 10;
		testData1.b = 20;
		testData1.c = 30;
		testData1.d = 40;
		testData1.e = 50;
		testData1.f = 60;

		TestDataProfiles2 testData2;

		testData2 = doTest(testData1, serializer1, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer1, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer2, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer2, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer1Profile, serializer1Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);
		testData2 = doTest(testData1, serializer1Profile, serializer2Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer2Profile, serializer2Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);
		testData2 = doTest(testData1, serializer2Profile, serializer1Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);
	}

	public static class TestDataRecursive {
		@Serialize
		public String s;

		@Serialize
		@SerializeNullable
		public TestDataRecursive next;

		public TestDataRecursive() {
		}

		public TestDataRecursive(String s) {
			this.s = s;
		}
	}

	@Test
	public void testDataRecursive() {
		TestDataRecursive testData1 = new TestDataRecursive("a");
		testData1.next = new TestDataRecursive("b");
		testData1.next.next = new TestDataRecursive("c");
		TestDataRecursive testData2 = doTest(TestDataRecursive.class, testData1);
		assertTrue(testData1 != testData2 && testData1.next.next != testData2.next.next);
		assertEquals(testData1.s, testData2.s);
		assertEquals(testData1.next.s, testData2.next.s);
		assertEquals(testData1.next.next.s, testData2.next.next.s);
		assertNull(testData2.next.next.next);
	}

	public static class TestDataExtraSubclasses {
		@Serialize
		@SerializeSubclasses(value = String.class, extraSubclassesId = "extraSubclasses1")
		public Object object1;

		@Serialize
		@SerializeSubclasses(value = String.class, extraSubclassesId = "extraSubclasses2")
		public Object object2;
	}

	@Test
	public void testDataExtraSubclasses() {
		TestDataExtraSubclasses testData1 = new TestDataExtraSubclasses();
		testData1.object1 = 10;
		testData1.object2 = "object2";

		BinarySerializer<TestDataExtraSubclasses> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.withSubclasses("extraSubclasses1", Integer.class)
				.withSubclasses(Object.class, asList(Boolean.class))
				.build(TestDataExtraSubclasses.class);
		TestDataExtraSubclasses testData2 = doTest(testData1, serializer);

		assertEquals(testData1.object1, testData2.object1);
		assertEquals(testData1.object2, testData2.object2);

		testData1.object1 = true;
		testData1.object2 = true;

		TestDataExtraSubclasses testData3 = doTest(testData1, serializer);
		assertEquals(testData1.object1, testData3.object1);
		assertEquals(testData1.object2, testData3.object2);
	}

	@SerializeSubclasses(value = TestDataExtraSubclasses1.class, extraSubclassesId = "extraSubclasses")
	public interface TestDataExtraSubclassesInterface {
	}

	public static class TestDataExtraSubclasses1 implements TestDataExtraSubclassesInterface {
		@Serialize
		public boolean z;
	}

	public static class TestDataExtraSubclasses2 implements TestDataExtraSubclassesInterface {
		@Serialize
		public int i;
	}

	public static class TestDataExtraSubclasses3 implements TestDataExtraSubclassesInterface {
		@Serialize
		public String s;
	}

	@Test
	public void testDataExtraSubclassesInterface() {
		TestDataExtraSubclassesInterface testData1 = new TestDataExtraSubclasses2();
		((TestDataExtraSubclasses2) testData1).i = 10;

		BinarySerializer<TestDataExtraSubclassesInterface> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.withSubclasses("extraSubclasses", TestDataExtraSubclasses2.class)
				.withSubclasses(TestDataExtraSubclassesInterface.class, asList(TestDataExtraSubclasses3.class))
				.build(TestDataExtraSubclassesInterface.class);
		TestDataExtraSubclassesInterface testData2 = doTest(testData1, serializer);

		assertEquals(((TestDataExtraSubclasses2) testData1).i, ((TestDataExtraSubclasses2) testData2).i);

		testData1 = new TestDataExtraSubclasses3();
		((TestDataExtraSubclasses3) testData1).s = "abc";

		TestDataExtraSubclassesInterface testData3 = doTest(testData1, serializer);

		assertEquals(((TestDataExtraSubclasses3) testData1).s, ((TestDataExtraSubclasses3) testData3).s);
	}

	@SerializeSubclasses
	public static abstract class TestDataAbstract {
		private final int position;

		TestDataAbstract(int position) {
			this.position = position;
		}

		@SerializeVarLength
		@Serialize(added = 1)
		public int getPosition() {
			return position;
		}
	}

	public static class TestDataAbstractImpl extends TestDataAbstract {
		public TestDataAbstractImpl(@Deserialize("position") int position) {
			super(position);
		}
	}

	public static class TestDataContainerOfAbstractData {
		@Serialize
		public TestDataAbstract data;
	}

	@Test
	public void testDataExtraSubclasses2() {
		TestDataAbstractImpl testImpl = new TestDataAbstractImpl(123);
		TestDataContainerOfAbstractData testData1 = new TestDataContainerOfAbstractData();
		testData1.data = testImpl;

		BinarySerializer<TestDataContainerOfAbstractData> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.withSubclasses(TestDataAbstract.class, asList(TestDataAbstractImpl.class))
				.build(TestDataContainerOfAbstractData.class);
		TestDataContainerOfAbstractData testData2 = doTest(testData1, serializer);

		assertEquals(testData1.data.getPosition(), testData2.data.getPosition());
	}

	public interface TestInheritAnnotationsInterface1 {
		@Serialize
		int getIntValue();
	}

	public interface TestInheritAnnotationsInterface2 {
		@Serialize
		double getDoubleValue();
	}

	public interface TestInheritAnnotationsInterface3 extends TestInheritAnnotationsInterface1, TestInheritAnnotationsInterface2 {
		@Serialize
		String getStringValue();
	}

	public static class TestInheritAnnotationsInterfacesImpl implements TestInheritAnnotationsInterface3 {
		public int i;
		public double d;
		public String s;

		@Override
		public String getStringValue() {
			return s;
		}

		@Override
		public int getIntValue() {
			return i;
		}

		@Override
		public double getDoubleValue() {
			return d;
		}
	}

	@Test
	public void testInheritSerialize() {
		TestInheritAnnotationsInterfacesImpl testData1 = new TestInheritAnnotationsInterfacesImpl();
		testData1.i = 10;
		testData1.d = 1.23;
		testData1.s = "test";

		TestInheritAnnotationsInterface3 testData2 = doTest(TestInheritAnnotationsInterface3.class, testData1);

		assertEquals(testData1.getIntValue(), testData2.getIntValue());
		assertEquals(testData1.getDoubleValue(), testData2.getDoubleValue(), Double.MIN_VALUE);
		assertEquals(testData1.getStringValue(), testData2.getStringValue());

		TestInheritAnnotationsInterfacesImpl testData3 = doTest(TestInheritAnnotationsInterfacesImpl.class, testData1);

		assertEquals(0, testData3.getIntValue());
		assertEquals(0.0, testData3.getDoubleValue(), Double.MIN_VALUE);
		assertNull(testData3.getStringValue());
	}

	public static class TestDataDeserializeFactory0 {
		String str;
		int hash;

		@Serialize
		public String getStr() {
			return str;
		}

		public int getHash() {
			return hash;
		}

		public static TestDataDeserializeFactory0 create(@Deserialize("str") String str) {
			TestDataDeserializeFactory0 testData = new TestDataDeserializeFactory0();
			testData.str = str;
			testData.hash = str.hashCode();
			return testData;
		}
	}

	public enum TestEnum {
		ONE(1), TWO(2), THREE(3);

		private final int value;

		TestEnum(int value) {
			this.value = value;
		}

		@Serialize
		public int getValue() {
			return value;
		}

		private static final Map<Integer, TestEnum> CACHE = new ConcurrentHashMap<>();

		static {
			for (TestEnum c : TestEnum.values()) {
				if (CACHE.put(c.value, c) != null)
					throw new IllegalStateException("Duplicate code " + c.value);
			}
		}

		public static TestEnum of(@Deserialize("value") int value) {
			return CACHE.get(value);
		}
	}

	@Test
	public void testCustomSerializeEnum() {
		TestEnum sourceData = TestEnum.TWO;
		TestEnum resultData = doTest(TestEnum.class, sourceData);
		assertEquals(sourceData, resultData);
	}

	public static class ListEnumHolder {
		@Serialize
		public List<TestEnum> list;
	}

	@Test
	public void testListEnums() {
		ListEnumHolder testData1 = new ListEnumHolder();
		testData1.list = asList(TestEnum.ONE, TestEnum.THREE, TestEnum.TWO);
		ListEnumHolder testData2 = doTest(ListEnumHolder.class, testData1);
		assertEquals(testData1.list, testData2.list);
	}

	public static class MapEnumHolder {
		@Serialize
		public Map<TestEnum, String> map;
	}

	@Test
	public void testMapEnums() {
		MapEnumHolder testData1 = new MapEnumHolder();
		testData1.map = new HashMap<>();
		testData1.map.put(TestEnum.ONE, "abc");
		testData1.map.put(TestEnum.TWO, "xyz");
		MapEnumHolder testData2 = doTest(MapEnumHolder.class, testData1);
		assertEquals(testData1.map, testData2.map);
		assertTrue(testData2.map instanceof EnumMap);
	}

	public enum TestEnum2 {
		ONE, TWO, THREE
	}

	public enum TestEnum127 {
		ITEM0, ITEM1, ITEM2, ITEM3, ITEM4, ITEM5, ITEM6, ITEM7, ITEM8, ITEM9,
		ITEM10, ITEM11, ITEM12, ITEM13, ITEM14, ITEM15, ITEM16, ITEM17, ITEM18, ITEM19,
		ITEM20, ITEM21, ITEM22, ITEM23, ITEM24, ITEM25, ITEM26, ITEM27, ITEM28, ITEM29,
		ITEM30, ITEM31, ITEM32, ITEM33, ITEM34, ITEM35, ITEM36, ITEM37, ITEM38, ITEM39,
		ITEM40, ITEM41, ITEM42, ITEM43, ITEM44, ITEM45, ITEM46, ITEM47, ITEM48, ITEM49,
		ITEM50, ITEM51, ITEM52, ITEM53, ITEM54, ITEM55, ITEM56, ITEM57, ITEM58, ITEM59,
		ITEM60, ITEM61, ITEM62, ITEM63, ITEM64, ITEM65, ITEM66, ITEM67, ITEM68, ITEM69,
		ITEM70, ITEM71, ITEM72, ITEM73, ITEM74, ITEM75, ITEM76, ITEM77, ITEM78, ITEM79,
		ITEM80, ITEM81, ITEM82, ITEM83, ITEM84, ITEM85, ITEM86, ITEM87, ITEM88, ITEM89,
		ITEM90, ITEM91, ITEM92, ITEM93, ITEM94, ITEM95, ITEM96, ITEM97, ITEM98, ITEM99,
		ITEM100, ITEM101, ITEM102, ITEM103, ITEM104, ITEM105, ITEM106, ITEM107, ITEM108, ITEM109,
		ITEM110, ITEM111, ITEM112, ITEM113, ITEM114, ITEM115, ITEM116, ITEM117, ITEM118, ITEM119,
		ITEM120, ITEM121, ITEM122, ITEM123, ITEM124, ITEM125, ITEM126
	}

	public enum TestEnum128 {
		ITEM0, ITEM1, ITEM2, ITEM3, ITEM4, ITEM5, ITEM6, ITEM7, ITEM8, ITEM9,
		ITEM10, ITEM11, ITEM12, ITEM13, ITEM14, ITEM15, ITEM16, ITEM17, ITEM18, ITEM19,
		ITEM20, ITEM21, ITEM22, ITEM23, ITEM24, ITEM25, ITEM26, ITEM27, ITEM28, ITEM29,
		ITEM30, ITEM31, ITEM32, ITEM33, ITEM34, ITEM35, ITEM36, ITEM37, ITEM38, ITEM39,
		ITEM40, ITEM41, ITEM42, ITEM43, ITEM44, ITEM45, ITEM46, ITEM47, ITEM48, ITEM49,
		ITEM50, ITEM51, ITEM52, ITEM53, ITEM54, ITEM55, ITEM56, ITEM57, ITEM58, ITEM59,
		ITEM60, ITEM61, ITEM62, ITEM63, ITEM64, ITEM65, ITEM66, ITEM67, ITEM68, ITEM69,
		ITEM70, ITEM71, ITEM72, ITEM73, ITEM74, ITEM75, ITEM76, ITEM77, ITEM78, ITEM79,
		ITEM80, ITEM81, ITEM82, ITEM83, ITEM84, ITEM85, ITEM86, ITEM87, ITEM88, ITEM89,
		ITEM90, ITEM91, ITEM92, ITEM93, ITEM94, ITEM95, ITEM96, ITEM97, ITEM98, ITEM99,
		ITEM100, ITEM101, ITEM102, ITEM103, ITEM104, ITEM105, ITEM106, ITEM107, ITEM108, ITEM109,
		ITEM110, ITEM111, ITEM112, ITEM113, ITEM114, ITEM115, ITEM116, ITEM117, ITEM118, ITEM119,
		ITEM120, ITEM121, ITEM122, ITEM123, ITEM124, ITEM125, ITEM126, ITEM127
	}

	public static class EnumPojo {
		@Serialize
		public TestEnum127 enum127NotNullable = TestEnum127.ITEM126;

		@Serialize
		@SerializeNullable
		public TestEnum127 enum127Nullable;

		@Serialize
		public TestEnum128 enum128NotNullable = TestEnum128.ITEM127;

		@Serialize
		@SerializeNullable
		public TestEnum128 enum128Nullable;
	}

	@Test
	public void testEnums() {
		TestEnum2 testData1 = TestEnum2.ONE;
		TestEnum2 testData2 = doTest(TestEnum2.class, testData1);
		assertEquals(testData1, testData2);

		BinarySerializer<EnumPojo> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER).build(EnumPojo.class);

		byte[] array = new byte[2000];
		EnumPojo enumPojoBefore = new EnumPojo();

		assertEquals(4, serializer.encode(array, 0, enumPojoBefore));
		EnumPojo enumPojoAfter = doTest(enumPojoBefore, serializer);

		assertEquals(enumPojoBefore.enum127NotNullable, enumPojoAfter.enum127NotNullable);
		assertEquals(enumPojoBefore.enum127Nullable, enumPojoAfter.enum127Nullable);
		assertEquals(enumPojoBefore.enum128NotNullable, enumPojoAfter.enum128NotNullable);
		assertEquals(enumPojoBefore.enum128Nullable, enumPojoAfter.enum128Nullable);

		enumPojoBefore.enum127Nullable = TestEnum127.ITEM126;
		enumPojoBefore.enum128Nullable = TestEnum128.ITEM127;
		assertEquals(5, serializer.encode(array, 0, enumPojoBefore));
		EnumPojo enumPojoAfter2 = doTest(enumPojoBefore, serializer);
		assertEquals(enumPojoBefore.enum127NotNullable, enumPojoAfter2.enum127NotNullable);
		assertEquals(enumPojoBefore.enum127Nullable, enumPojoAfter2.enum127Nullable);
		assertEquals(enumPojoBefore.enum128NotNullable, enumPojoAfter2.enum128NotNullable);
		assertEquals(enumPojoBefore.enum128Nullable, enumPojoAfter2.enum128Nullable);
	}

	public static class ListEnumHolder2 {
		@Serialize
		public List<@SerializeNullable TestEnum2> list;
	}

	@Test
	public void testListEnums2() {
		ListEnumHolder2 testData1 = new ListEnumHolder2();
		testData1.list = asList(TestEnum2.ONE, TestEnum2.THREE, null, TestEnum2.TWO, null);
		ListEnumHolder2 testData2 = doTest(ListEnumHolder2.class, testData1);
		assertEquals(testData1.list, testData2.list);
	}

	public static class MapEnumHolder2 {
		@Serialize
		public Map<TestEnum2, TestEnum2> map;
	}

	@Test
	public void testMapEnums2() {
		MapEnumHolder2 testData1 = new MapEnumHolder2();
		testData1.map = new HashMap<>();
		testData1.map.put(TestEnum2.ONE, TestEnum2.THREE);
		testData1.map.put(TestEnum2.TWO, TestEnum2.ONE);
		MapEnumHolder2 testData2 = doTest(MapEnumHolder2.class, testData1);
		assertEquals(testData1.map, testData2.map);
		assertTrue(testData2.map instanceof EnumMap);
	}

	public static class SetIntegerHolder {
		@Serialize
		public Set<Integer> set;
	}

	@Test
	public void testSet() {
		SetIntegerHolder testData1 = new SetIntegerHolder();
		testData1.set = new HashSet<>();
		testData1.set.add(1);
		testData1.set.add(2);
		SetIntegerHolder testData2 = doTest(SetIntegerHolder.class, testData1);
		assertEquals(testData1.set, testData2.set);
		assertTrue(testData2.set instanceof LinkedHashSet);
	}

	public static class EnumSetHolder {
		@Serialize
		public Set<TestEnum> set;

		@Nullable
		@Serialize
		@SerializeNullable
		public Set<TestEnum> setNullable;
	}

	@Test
	public void testEnumSet() {
		EnumSetHolder testData1 = new EnumSetHolder();
		testData1.set = EnumSet.copyOf(asList(TestEnum.THREE, TestEnum.ONE));
		EnumSetHolder testData2 = doTest(EnumSetHolder.class, testData1);
		assertEquals(testData1.set, testData2.set);
		assertTrue(testData2.set instanceof EnumSet);

		// test for empty set with null
		EnumSetHolder testEmpty1 = new EnumSetHolder();
		testEmpty1.set = EnumSet.noneOf(TestEnum.class);
		testEmpty1.setNullable = null;
		EnumSetHolder testEmpty2 = doTest(EnumSetHolder.class, testEmpty1);
		assertEquals(testEmpty1.set, testEmpty2.set);
		assertEquals(testEmpty1.setNullable, testEmpty2.setNullable);
		assertTrue(testEmpty2.set instanceof EnumSet);
		assertNull(testEmpty2.setNullable);

		// test for empty set without null
		testEmpty1.setNullable = EnumSet.noneOf(TestEnum.class);
		testEmpty2 = doTest(EnumSetHolder.class, testEmpty1);
		assertEquals(testEmpty1.set, testEmpty2.set);
		assertEquals(testEmpty1.setNullable, testEmpty2.setNullable);
		assertTrue(testEmpty2.set instanceof EnumSet);
		assertTrue(testEmpty2.setNullable instanceof EnumSet);
	}

	public static class Generic<T> {
		@Serialize
		public T item;
	}

	public static class GenericHolder1 extends Generic<Integer> {
	}

	public static class GenericHolder2 extends Generic<String> {
	}

	public static class GenericComplex {
		@Serialize
		public GenericHolder1 holder1;

		@Serialize
		public GenericHolder2 holder2;
	}

	@Test
	public void testGenericHolder() {
		GenericComplex gc = new GenericComplex();
		GenericHolder1 g1 = new GenericHolder1();
		g1.item = 42;
		GenericHolder2 g2 = new GenericHolder2();
		g2.item = "abcd";

		gc.holder1 = g1;
		gc.holder2 = g2;

		GenericComplex _gc = doTest(GenericComplex.class, gc);

		assertEquals(gc.holder1.item, _gc.holder1.item);
		assertEquals(gc.holder2.item, _gc.holder2.item);
	}

	public static class TestObj {
		@Serialize
		public String string;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestObj testObj = (TestObj) o;

			if (!Objects.equals(string, testObj.string)) return false;
			return Objects.equals(integer, testObj.integer);

		}

		@Override
		public int hashCode() {
			int result = string != null ? string.hashCode() : 0;
			result = 31 * result + (integer != null ? integer.hashCode() : 0);
			return result;
		}

		@Serialize
		public Integer integer;

		public TestObj(@Deserialize("string") String string, @Deserialize("integer") Integer integer) {
			this.string = string;
			this.integer = integer;
		}
	}

	public static class ListOfObjectHolder {
		@Serialize
		public List<TestObj> list;
	}

	@Test
	public void testListObj() {
		ListOfObjectHolder testData1 = new ListOfObjectHolder();
		testData1.list = asList(new TestObj("a", 1), new TestObj("b", 2), new TestObj("c", 3));
		ListOfObjectHolder testData2 = doTest(ListOfObjectHolder.class, testData1);
		assertEquals(testData1.list, testData2.list);
	}

	public static class TestConstructorWithBoolean {
		@Serialize(added = 1)
		public final String url;
		@Serialize(added = 2)
		public final boolean resolve;

		public TestConstructorWithBoolean(@Deserialize("url") String url, @Deserialize("resolve") boolean resolve) {
			this.url = url;
			this.resolve = resolve;
		}
	}

	@Test
	public void testConstructorWithBoolean() {
		TestConstructorWithBoolean test = new TestConstructorWithBoolean("abc", true);

		BinarySerializer<TestConstructorWithBoolean> serializer1 = SerializerBuilder
				.create(DEFINING_CLASS_LOADER)
				.build(TestConstructorWithBoolean.class);

		BinarySerializer<TestConstructorWithBoolean> serializer2 = SerializerBuilder
				.create(DEFINING_CLASS_LOADER)
				.withEncodeVersion(1)
				.build(TestConstructorWithBoolean.class);

		TestConstructorWithBoolean _test = doTest(test, serializer1);
		assertEquals(test.resolve, _test.resolve);
		assertEquals(test.url, _test.url);

		TestConstructorWithBoolean _test2 = doTest(test, serializer2);
		assertEquals(test.url, _test2.url);
	}

	public static class TestInetAddress {
		@Serialize
		public InetAddress inetAddress;

		@Serialize
		public Inet4Address inet4Address;

		@Serialize
		public Inet6Address inet6Address;

		@Serialize
		@SerializeClass(SerializerDefInet4Address.class)
		public InetAddress inetAddress2;
	}

	@Test
	public void testInetAddress() throws UnknownHostException {
		TestInetAddress testInetAddress = new TestInetAddress();
		testInetAddress.inetAddress = Inet6Address.getByName("2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d");
		testInetAddress.inet4Address = (Inet4Address) Inet4Address.getByName("127.0.0.1");
		testInetAddress.inet6Address = (Inet6Address) Inet6Address.getByName("2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d");
		testInetAddress.inetAddress2 = Inet4Address.getByName("127.0.0.1");

		TestInetAddress genTestInetAddress = doTest(TestInetAddress.class, testInetAddress);

		assertEquals(testInetAddress.inetAddress, genTestInetAddress.inetAddress);
		assertEquals(testInetAddress.inet4Address, genTestInetAddress.inet4Address);
		assertEquals(testInetAddress.inet6Address, genTestInetAddress.inet6Address);
		assertEquals(testInetAddress.inetAddress2, genTestInetAddress.inetAddress2);

		testInetAddress.inetAddress = Inet4Address.getByName("127.0.0.1");

		TestInetAddress genTestInetAddress2 = doTest(TestInetAddress.class, testInetAddress);

		assertEquals(testInetAddress.inetAddress, genTestInetAddress2.inetAddress);
		assertEquals(testInetAddress.inet4Address, genTestInetAddress2.inet4Address);
		assertEquals(testInetAddress.inet6Address, genTestInetAddress2.inet6Address);
		assertEquals(testInetAddress.inetAddress2, genTestInetAddress.inetAddress2);
	}

	public static class TestObject {
		public enum TestEnum {
			ONE(1), TWO(2), THREE(3);

			TestEnum(@SuppressWarnings("UnusedParameters") int id) {
			}
		}

		@Serialize
		@SerializeClass(SerializerDefBoolean.class)
		public Object zBoxed;
		@Serialize
		@SerializeClass(SerializerDefChar.class)
		public Object cBoxed;
		@Serialize
		@SerializeClass(SerializerDefByte.class)
		public Object bBoxed;
		@Serialize
		@SerializeClass(SerializerDefShort.class)
		public Object sBoxed;
		@Serialize
		@SerializeClass(SerializerDefInt.class)
		public Object iBoxed;
		@Serialize
		@SerializeClass(SerializerDefLong.class)
		public Object lBoxed;
		@Serialize
		@SerializeClass(SerializerDefFloat.class)
		public Object fBoxed;
		@Serialize
		@SerializeClass(SerializerDefDouble.class)
		public Object dBoxed;

		@Serialize
		@SerializeClass(SerializerDefString.class)
		public Object string;
		@Serialize
		@SerializeSubclasses({Inet4Address.class, Inet6Address.class})
		public Object address;

		@Serialize
		@SerializeSubclasses({Inet4Address.class, Inet6Address.class})
		public Object address2;

		@Serialize
		public List<@SerializeClass(SerializerDefInt.class) Object> list;
	}

	@Test
	public void testObject() throws UnknownHostException {
		TestObject testData1 = new TestObject();

		testData1.zBoxed = true;
		testData1.cBoxed = Character.MAX_VALUE;
		testData1.bBoxed = Byte.MIN_VALUE;
		testData1.sBoxed = Short.MIN_VALUE;
		testData1.iBoxed = Integer.MIN_VALUE;
		testData1.lBoxed = Long.MIN_VALUE;
		testData1.fBoxed = Float.MIN_VALUE;
		testData1.dBoxed = Double.MIN_VALUE;

		testData1.string = "abc";
		testData1.address = InetAddress.getByName("127.0.0.1");
		testData1.address2 = InetAddress.getByName("2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d");

		testData1.list = Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE);

		TestObject testData2 = doTest(TestObject.class, testData1);

		assertEquals(testData1.zBoxed, testData2.zBoxed);
		assertEquals(testData1.cBoxed, testData2.cBoxed);
		assertEquals(testData1.bBoxed, testData2.bBoxed);
		assertEquals(testData1.sBoxed, testData2.sBoxed);
		assertEquals(testData1.iBoxed, testData2.iBoxed);
		assertEquals(testData1.lBoxed, testData2.lBoxed);
		assertEquals(testData1.fBoxed, testData2.fBoxed);
		assertEquals(testData1.dBoxed, testData2.dBoxed);

		assertEquals(testData1.string, testData2.string);
		assertEquals(testData1.address, testData2.address);
		assertEquals(testData1.address2, testData2.address2);

		assertEquals(testData1.list.size(), testData2.list.size());
		for (int i = 0; i < testData1.list.size(); i++) {
			assertEquals(testData1.list.get(i), testData2.list.get(i));
		}
	}

	public static class NullableOpt {
		@Serialize
		public byte @SerializeNullable [] bytes;

		@Serialize
		public int @SerializeNullable [] ints;

		@Serialize
		@SerializeNullable
		public TestEnum2 testEnum2;

		@Serialize
		@SerializeNullable
		public List<String> list;

		@Serialize
		@SerializeNullable
		public Map<String, String> map;

		@Serialize
		@SerializeNullable
		public Set<String> set;

		@Serialize
		@SerializeNullable
		@SerializeSubclasses({TestEnum2.class, String.class})
		public Object subclass;
	}

	@Test
	public void testNullableOpt() {
		BinarySerializer<NullableOpt> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER).build(NullableOpt.class);

		byte[] array = new byte[2000];
		NullableOpt nullableOpt1 = new NullableOpt();

		assertEquals(7, serializer.encode(array, 0, nullableOpt1));

		NullableOpt nullableOpt2 = doTest(nullableOpt1, serializer);

		assertEquals(nullableOpt1.bytes, nullableOpt2.bytes);
		assertEquals(nullableOpt1.ints, nullableOpt2.ints);
		assertEquals(nullableOpt1.list, nullableOpt2.list);
		assertEquals(nullableOpt1.map, nullableOpt2.map);
		assertEquals(nullableOpt1.set, nullableOpt2.set);
		assertEquals(nullableOpt1.testEnum2, nullableOpt2.testEnum2);

		nullableOpt1.bytes = new byte[]{};
		nullableOpt1.ints = new int[]{};
		nullableOpt1.list = new ArrayList<>();
		nullableOpt1.map = new HashMap<>();
		nullableOpt1.set = new HashSet<>();
		nullableOpt1.subclass = TestEnum2.ONE;
		nullableOpt1.testEnum2 = TestEnum2.ONE;

		assertEquals(8, serializer.encode(array, 0, nullableOpt1));

		NullableOpt nullableOpt3 = doTest(nullableOpt1, serializer);

		assertArrayEquals(nullableOpt1.bytes, nullableOpt3.bytes);
		assertArrayEquals(nullableOpt1.ints, nullableOpt3.ints);
		assertEquals(nullableOpt1.list, nullableOpt3.list);
		assertEquals(nullableOpt1.map, nullableOpt3.map);
		assertEquals(nullableOpt1.set, nullableOpt3.set);
		assertEquals(nullableOpt1.testEnum2, nullableOpt3.testEnum2);
	}

	public static class TestGetterVersion {
		private final String str;
		private final List<String> strings;

		private TestGetterVersion(String str, List<String> strings) {
			this.str = str;
			this.strings = strings;
		}

		public static TestGetterVersion of(@Deserialize("str") String str, @Deserialize("strings") List<String> strings) {
			return new TestGetterVersion(str, strings);
		}

		@Serialize(added = 1)
		public String getStr() {
			return str;
		}

		@Serialize(added = 2)
		public List<String> getStrings() {
			return strings;
		}
	}

	@Test
	public void testVersionGetter() {
		TestGetterVersion test = TestGetterVersion.of("test", asList("a", "b"));
		BinarySerializer<TestGetterVersion> serializerV1 = SerializerBuilder
				.create(DEFINING_CLASS_LOADER)
				.withEncodeVersion(1)
				.build(TestGetterVersion.class);

		TestGetterVersion _testV1 = doTest(test, serializerV1);

		assertEquals(test.getStr(), _testV1.getStr());

		BinarySerializer<TestGetterVersion> serializerV2 = SerializerBuilder
				.create(DEFINING_CLASS_LOADER)
				.withEncodeVersion(2)
				.build(TestGetterVersion.class);

		TestGetterVersion _testV2 = doTest(test, serializerV2);

		assertEquals(test.getStrings(), _testV2.getStrings());
		assertEquals(test.getStr(), _testV2.getStr());
	}

	public static class StringWrapper {
		private final String str;

		public StringWrapper(@Deserialize("str") String str) {
			this.str = str;
		}

		@Serialize
		public String getStr() {
			return str;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			StringWrapper that = (StringWrapper) o;

			return Objects.equals(str, that.str);

		}

		@Override
		public int hashCode() {
			return str != null ? str.hashCode() : 0;
		}
	}

	public static class CustomArrayHolder {
		private StringWrapper[] stringWrappers;

		public CustomArrayHolder(@Deserialize("stringWrappers") StringWrapper[] stringWrappers) {
			this.stringWrappers = stringWrappers;
		}

		public void setStringWrappers(StringWrapper[] stringWrappers) {
			this.stringWrappers = stringWrappers;
		}

		@Serialize
		public StringWrapper[] getStringWrappers() {
			return stringWrappers;
		}
	}

	@Test
	public void testArrayOfCustomClasses() {
		BinarySerializer<CustomArrayHolder> serializer = SerializerBuilder
				.create(DEFINING_CLASS_LOADER)
				.build(CustomArrayHolder.class);
		StringWrapper[] array = {new StringWrapper("str"), new StringWrapper("abc")};
		CustomArrayHolder holder = new CustomArrayHolder(array);
		CustomArrayHolder _holder = doTest(holder, serializer);

		assertArrayEquals(holder.getStringWrappers(), _holder.getStringWrappers());
	}

	@FunctionalInterface
	interface TestInterface {
		Object getValue();
	}

	public static class Imp implements TestInterface {
		private Integer value;

		@Override
		@Serialize(added = 0)
		@SerializeNullable
		public Integer getValue() {
			return value;
		}

		public void setValue(Integer value) {
			this.value = value;
		}
	}

	@Test
	public void testOverridenBridgeMethod() {
		Imp object = new Imp();
		object.setValue(100);
		Imp deserialized = doTest(Imp.class, object);
		assertEquals(100, deserialized.getValue().intValue());
	}

	@FunctionalInterface
	interface TestGenericInterface<T> {
		T getValue();
	}

	public static class GenericImp implements TestGenericInterface<Integer> {
		private Integer value;

		@Override
		@Serialize(added = 0)
		@SerializeNullable
		public Integer getValue() {
			return value;
		}

		public void setValue(Integer value) {
			this.value = value;
		}
	}

	@Test
	public void testGenericBridgeMethod() {
		GenericImp object = new GenericImp();
		object.setValue(100);
		GenericImp deserialized = doTest(GenericImp.class, object);
		assertEquals(100, deserialized.getValue().intValue());
	}

	public static class TestDataFromVersion3 {
		@Serialize(added = 3)
		public int a;

		@Serialize(added = 4, removed = 5)
		public int b;

		@Serialize(added = 5)
		public int c;
	}

	@Test
	public void testUnsupportedVersion() {
		SerializerBuilder builder = SerializerBuilder.create(DEFINING_CLASS_LOADER);
		BinarySerializer<TestDataFromVersion3> serializer = builder.build(TestDataFromVersion3.class);

		TestDataFromVersion3 testDataBefore = new TestDataFromVersion3();
		testDataBefore.a = 10;
		testDataBefore.b = 20;
		testDataBefore.c = 30;

		byte[] array = new byte[1000];
		serializer.encode(array, 0, testDataBefore);
		assertEquals(5, array[0]);
		byte unsupportedVersion = 123;
		array[0] = unsupportedVersion; // overriding version

		try {
			serializer.decode(array, 0);
			fail();
		} catch (CorruptedDataException e) {
			assertEquals("Unsupported version: " + unsupportedVersion + ", supported versions: [3, 4, 5]", e.getMessage());
		}
	}

	@SerializeSubclasses({TestNullableInterfaceImpl.class})
	public interface TestNullableInterface {
	}

	public static class TestNullableInterfaceImpl implements TestNullableInterface {
	}

	public static class Container {
		@Serialize
		@SerializeNullable
		public TestNullableInterface obj;
	}

	@Test
	public void testNullableSubclass() {
		BinarySerializer<Container> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER).build(Container.class);
		Container container = new Container();
		Container container2 = doTest(Container.class, container);
		assertEquals(container.obj, container2.obj);
	}

	@Test
	public void testMorePreciseFieldType() {
		LinkedListHolderImpl testData1 = new LinkedListHolderImpl(asList("first", "second", "third"));
		LinkedListHolder testData2 = doTest(LinkedListHolderImpl.class, testData1);

		assertEquals(testData1.list(), testData2.list());
	}

	@Test
	public void testCollection() {
		List<String> list = new ArrayList<>();
		list.add("one");
		list.add("two");
		list.add("three");
		Collection<String> collection = Collections.unmodifiableCollection(list);

		assertThat(collection, not(instanceOf(List.class)));

		CollectionHolder collectionHolder = new CollectionHolder();
		collectionHolder.collection = collection;

		CollectionHolder deserialized = doTest(CollectionHolder.class, collectionHolder);

		assertEquals(new ArrayList<>(collection), new ArrayList<>(deserialized.collection));
	}

	@Test
	public void testQueue() {
		Queue<String> queue = new ArrayDeque<>();
		queue.add("one");
		queue.add("two");
		queue.add("three");

		QueueHolder queueHolder = new QueueHolder();
		queueHolder.queue = queue;

		QueueHolder deserialized = doTest(QueueHolder.class, queueHolder);

		assertEquals(new ArrayList<>(queue), new ArrayList<>(deserialized.queue));
	}

	@Test
	public void testSets() {
		Set<String> regular = new TreeSet<>();

		assertThat(regular, not(instanceOf(HashSet.class)));

		regular.add("a");
		regular.add("b");
		regular.add("c");

		HashSet<String> hash = new HashSet<>();
		hash.add("d");
		hash.add("e");
		hash.add("f");

		LinkedHashSet<String> linked = new LinkedHashSet<>();
		linked.add("g");
		linked.add("h");
		linked.add("i");

		SetsHolder setsHolder = new SetsHolder();
		setsHolder.regular = regular;
		setsHolder.hash = hash;
		setsHolder.linked = linked;

		SetsHolder deserialized = doTest(SetsHolder.class, setsHolder);

		assertEquals(regular, deserialized.regular);
		assertSame(LinkedHashSet.class, deserialized.regular.getClass());

		assertEquals(hash, deserialized.hash);
		assertSame(HashSet.class, deserialized.hash.getClass());

		assertEquals(linked, deserialized.linked);
		assertSame(LinkedHashSet.class, deserialized.linked.getClass());
	}

	@Test
	public void testMaps() {
		Map<Integer, String> regular = new TreeMap<>();

		assertThat(regular, not(instanceOf(HashMap.class)));

		regular.put(100, "a");
		regular.put(50, "b");
		regular.put(200, "c");

		HashMap<Integer, String> hash = new HashMap<>();
		hash.put(120, "d");
		hash.put(70, "e");
		hash.put(230, "f");

		LinkedHashMap<Integer, String> linked = new LinkedHashMap<>();
		linked.put(14, "g");
		linked.put(200, "h");
		linked.put(4, "i");

		MapsHolder mapsHolder = new MapsHolder();
		mapsHolder.regular = regular;
		mapsHolder.hash = hash;
		mapsHolder.linked = linked;

		MapsHolder deserialized = doTest(MapsHolder.class, mapsHolder);

		assertEquals(regular, deserialized.regular);
		assertSame(LinkedHashMap.class, deserialized.regular.getClass());

		assertEquals(hash, deserialized.hash);
		assertSame(HashMap.class, deserialized.hash.getClass());

		assertEquals(linked, deserialized.linked);
		assertSame(LinkedHashMap.class, deserialized.linked.getClass());
	}

	public interface LinkedListHolder {
		LinkedList<String> list();
	}

	public static class CollectionHolder {
		@Serialize
		public Collection<String> collection;
	}

	public static class QueueHolder {
		@Serialize
		public Queue<String> queue;
	}

	public static class SetsHolder {
		@Serialize
		public Set<String> regular;

		@Serialize
		public HashSet<String> hash;

		@Serialize
		public LinkedHashSet<String> linked;
	}

	public static class MapsHolder {
		@Serialize
		public Map<Integer, String> regular;

		@Serialize
		public HashMap<Integer, String> hash;

		@Serialize
		public LinkedHashMap<Integer, String> linked;
	}

	public static class LinkedListHolderImpl implements LinkedListHolder {
		@Serialize
		public final LinkedList<String> list;

		public LinkedListHolderImpl(@Deserialize("list") List<String> list) {
			this.list = new LinkedList<>(list);
		}

		@Override
		public LinkedList<String> list() {
			return this.list;
		}
	}
}
