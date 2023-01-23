package io.activej.serializer;

import io.activej.serializer.annotations.CustomAnnotations;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static io.activej.serializer.Utils.doTest;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AnnotationAliasesTest {

	@Retention(RetentionPolicy.RUNTIME)
	@Target({
			ElementType.FIELD,
			ElementType.METHOD,
			ElementType.TYPE_USE})
	public @interface N {
	}

	public static class TestDataNullables {
		@Serialize
		@N
		public String nullableString1;

		@Serialize
		@N
		public String nullableString2;

		@Serialize
		public List<@N String> listOfNullableStrings;

		@Serialize
		@N
		public String @N [] @N [] nullableArrayOfNullableArrayOfNullableStrings;

		@Serialize
		public Map<@N Integer, @N String> mapOfNullableInt2NullableString;
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

		BinarySerializer<TestDataNullables> serializer = SerializerFactory.builder()
				.withAnnotationAlias(SerializeNullable.class, N.class, n -> CustomAnnotations.serializeNullable())
				.build()
				.create(DEFINING_CLASS_LOADER, TestDataNullables.class);
		TestDataNullables testData2 = doTest(testData1, serializer);

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
}
