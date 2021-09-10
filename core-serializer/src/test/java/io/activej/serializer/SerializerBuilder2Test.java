package io.activej.serializer;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeVarLength;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.types.TypeT;
import org.junit.Rule;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static io.activej.serializer.Utils.AT_LEAST_JAVA_9;
import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class SerializerBuilder2Test {
	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	public static <T> T doTest(Class<T> type, T testData1) {
		return io.activej.serializer.Utils.doTest(testData1, createBuilder().build(type));
	}

	public static <T> T doTest(TypeT<T> type, T testData1) {
		return io.activej.serializer.Utils.doTest(testData1, createBuilder().build(type.getAnnotatedType()));
	}

	private static SerializerBuilder createBuilder() {
		return SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.withSubclasses("extraSubclasses1", asList(Integer.class, String.class));
	}

	public static class TestDataScalars {
		@Serialize
		public int i;

		@Serialize
		@SerializeNullable
		public Integer iBoxed;

		@Serialize
		public String string;
	}

	@Test
	public void testScalars() {
		TestDataScalars testData1 = new TestDataScalars();

		Random rnd = new Random();

		testData1.i = rnd.nextInt();
		testData1.iBoxed = Integer.MIN_VALUE;
		testData1.string = "abc";

		TestDataScalars testData2 = doTest(TestDataScalars.class, testData1);

		assertEquals(testData1.i, testData2.i);
		assertEquals(testData1.iBoxed, testData2.iBoxed);
		assertEquals(testData1.string, testData2.string);
	}

	@Test
	public void testList() {
		{
			List<Integer> testData1 = Arrays.asList(1, 2, 3);
			List<Integer> testData2 = doTest(new TypeT<List<Integer>>() {}, testData1);
			assertEquals(testData1, testData2);
		}

		assumeTrue("Prior to Java 9, some complex annotation paths are not picked up by JVM", AT_LEAST_JAVA_9);
		{
			List<Integer> testData1 = Arrays.asList(1, 2, null, 3);
			List<Integer> testData2 = doTest(new TypeT<List<@SerializeNullable Integer>>() {}, testData1);
			assertEquals(testData1, testData2);
		}

		{
			List<String> testData1 = Arrays.asList("1", "2", null, "3");
			List<String> testData2 = doTest(new TypeT<List<@SerializeNullable String>>() {}, testData1);
			assertEquals(testData1, testData2);
		}

		{
			List<String> testData1 = null;
			List<String> testData2 = doTest(new TypeT<@SerializeNullable List<@SerializeNullable String>>() {}, testData1);
			assertEquals(testData1, testData2);
		}
	}

	@Test
	public void testArray() {
		{
			int[] testData1 = new int[]{1, 2, 3};
			int[] testData2 = doTest(new TypeT<int[]>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}
		{
			int[] testData1 = new int[]{1, 2, 3};
			int[] testData2 = doTest(new TypeT<@SerializeVarLength int[]>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}
		{
			Integer[] testData1 = new Integer[]{1, 2, 3};
			Integer[] testData2 = doTest(new TypeT<Integer[]>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}
		{
			Integer[] testData1 = new Integer[]{1, 2, 3};
			Integer[] testData2 = doTest(new TypeT<@SerializeVarLength Integer[]>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}
		{
			Integer[][] testData1 = new Integer[][]{new Integer[]{0}, new Integer[]{1, 2, 3}, new Integer[]{4, 5, 6}};
			Integer[][] testData2 = doTest(new TypeT<Integer[][]>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}

		assumeTrue("Prior to Java 9, some complex annotation paths are not picked up by JVM", AT_LEAST_JAVA_9);
		{
			Integer[] testData1 = new Integer[]{1, 2, null, 3};
			Integer[] testData2 = doTest(new TypeT<@SerializeNullable @SerializeVarLength Integer[]>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}

		{
			Integer[][] testData1 = new Integer[][]{null, new Integer[]{null, 0}, new Integer[]{1, 2, 3}, new Integer[]{4, 5, 6}};
			Integer[][] testData2 = doTest(new TypeT<@SerializeVarLength @SerializeNullable Integer[] @SerializeNullable []>() {}, testData1);
			assertArrayEquals(testData1, testData2);
		}
	}

	public enum TestEnum1 {
		ONE, TWO
	}

	@Test
	public void testEnum() {
		{
			TestEnum1 testData1 = TestEnum1.ONE;
			TestEnum1 testData2 = doTest(TestEnum1.class, testData1);
			assertEquals(testData1, testData2);
		}

		assumeTrue("Prior to Java 9, some complex annotation paths are not picked up by JVM", AT_LEAST_JAVA_9);
		{
			TestEnum1 testData1 = null;
			TestEnum1 testData2 = doTest(new TypeT<@SerializeNullable TestEnum1>() {}, testData1);
			assertEquals(testData1, testData2);
		}
	}

	@Test
	public void testSubclasses() {
		assumeTrue("Prior to Java 9, some complex annotation paths are not picked up by JVM", AT_LEAST_JAVA_9);

		{
			Object testData1 = 1;
			Object testData2 = doTest(new TypeT<@SerializeClass(subclasses = {String.class, Integer.class}) Object>() {}, testData1);
			assertEquals(testData1, testData2);
		}
		{
			Object testData1 = "abc";
			Object testData2 = doTest(new TypeT<@SerializeClass(subclasses = {String.class, Integer.class}) Object>() {}, testData1);
			assertEquals(testData1, testData2);
		}
		{
			Object testData1 = null;
			Object testData2 = doTest(new TypeT<@SerializeClass(subclasses = {String.class, Integer.class}) @SerializeNullable Object>() {}, testData1);
			assertEquals(testData1, testData2);
		}
		{
			Object testData1 = 1;
			Object testData2 = doTest(new TypeT<@SerializeClass(subclassesId = "extraSubclasses1") Object>() {}, testData1);
			assertEquals(testData1, testData2);
		}
	}

	@Test
	public void testInetAddress() throws UnknownHostException {
		{
			InetAddress testData1 = Inet4Address.getByName("192.168.1.1");
			Object testData2 = doTest(new TypeT<InetAddress>() {}, testData1);
			assertEquals(testData1, testData2);
		}
	}

	public static class TestNode {
		@Serialize
		@SerializeNullable
		public TestNode left;

		@Serialize
		@SerializeNullable
		public TestNode right;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestNode node = (TestNode) o;
			return Objects.equals(left, node.left) && Objects.equals(right, node.right);
		}

		@Override
		public int hashCode() {
			int result = 0;
			result = 31 * result + (left != null ? left.hashCode() : 0);
			result = 31 * result + (right != null ? right.hashCode() : 0);
			return result;
		}
	}

	@Test
	public void testRecursiveTypes() {
		{
			TestNode testData1 = new TestNode();
			TestNode testData2 = doTest(new TypeT<TestNode>() {}, testData1);
			assertEquals(testData1, testData2);
		}
		{
			TestNode testData1 = new TestNode();
			testData1.left = new TestNode();
			testData1.left.right = new TestNode();
			testData1.right = new TestNode();
			testData1.right.left = new TestNode();
			TestNode testData2 = doTest(new TypeT<TestNode>() {}, testData1);
			assertEquals(testData1, testData2);
		}

		assumeTrue("Prior to Java 9, some complex annotation paths are not picked up by JVM", AT_LEAST_JAVA_9);
		{
			TestNode testData1 = null;
			TestNode testData2 = doTest(new TypeT<@SerializeNullable TestNode>() {}, testData1);
			assertEquals(testData1, testData2);
		}
	}
}
