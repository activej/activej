package io.activej.json;

import io.activej.common.exception.MalformedDataException;
import io.activej.json.annotations.JsonNullable;
import io.activej.types.TypeT;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JsonCodecFactoryTest {

	@SuppressWarnings("Convert2Diamond")
	@Test
	public void test() throws MalformedDataException {
		JsonCodecFactory factory = JsonCodecFactory.defaultInstance();
/*

		doTest(factory.resolve(String.class), "abc");
		doTest(factory.resolve(new TypeT<String>() {}), "abc");
		doTest(factory.resolve(new TypeT<>() {}), "abc");
		doTest(factory.resolve(new TypeT<Integer>() {}), 123);
		doTest(factory.resolve(new TypeT<List<Integer>>() {}), List.of(1, 2, 3));
		doTest(factory.resolve(new TypeT<>() {}), List.of(1, 2, 3));

		doTest(factory.resolve(new TypeT<@JsonNullable String>() {}), "abc");
		doTest(factory.resolve(new TypeT<@JsonNullable String>() {}), null);
		doTest(factory.resolve(new TypeT<List<@JsonNullable Integer>>() {}), Arrays.asList(1, null, 2, 3));

		assertThrows(NullPointerException.class, () ->
			doTest(factory.resolve(new TypeT<String>() {}), null)
		);

		assertThrows(NullPointerException.class, () ->
			doTest(factory.resolve(new TypeT<List<Integer>>() {}), Arrays.asList(1, null, 2, 3))
		);

		assertThrows(NullPointerException.class, () ->
			doTest(factory.resolve(new TypeT<@JsonNullable List<Integer>>() {}), Arrays.asList(1, null, 2, 3))
		);
		doTest(factory.resolve(new TypeT<@JsonNullable List<@JsonNullable Integer>>() {}), Arrays.asList(1, null, 2, 3));

		doTest(
			factory.resolve(
				new TypeT<List<@JsonSubclasses({Integer.class, String.class}) Object>>() {}
			),
			Arrays.asList(1, "abc"));

		doTest(
			factory.resolve(
				new TypeT<List<@JsonSubclasses({Integer.class, String.class}) @JsonNullable Object>>() {}
			),
			Arrays.asList(1, "abc", null));

		doTest(
			factory.resolve(
				new TypeT<List<@JsonSubclasses(value = {Integer.class, String.class}, tags = {"number", "string"}) Object>>() {}
			),
			Arrays.asList(1, "abc"));
*/

		doTest(
			factory.resolve(
				new TypeT<Map<String, Integer>>() {}
			),
			Map.of("abc", 123, "def", 456));

		doTest(
			factory.resolve(
				new TypeT<Map<Integer, Integer>>() {}
			),
			Map.of(1, 123, 2, 456));

		doTest(
			factory.resolve(
				new TypeT<Map<Byte, Integer>>() {}
			),
			Map.of((byte) 1, 123, (byte) 2, 456));
	}

	record TestRecord1(int n, String s) {}

	record TestRecord2<T>(int n, T t) {}

	@Test
	public void test2() throws MalformedDataException {
		JsonCodecFactory factory0 = JsonCodecFactory.defaultInstance();
		JsonCodecFactory factory1 = factory0.rebuild()
			.with(TestRecord1.class, ctx -> JsonCodecs.ofObject(TestRecord1::new,
				"n", TestRecord1::n, JsonCodecs.ofInteger(),
				"s", TestRecord1::s, JsonCodecs.ofString()))
			.build();

		//noinspection Convert2Diamond
		doTest(
			factory1.resolve(new TypeT<List<@JsonNullable TestRecord1>>() {}),
			Arrays.asList(new TestRecord1(123, "abc"), null, new TestRecord1(456, "def")));

		JsonCodecFactory factory2 = factory1.rebuild()
			.with(TestRecord2.class, ctx -> JsonCodecs.ofObject(TestRecord2::new,
				"n", TestRecord2::n, JsonCodecs.ofInteger(),
				"t", TestRecord2::t, ctx.scanTypeArgument(0)))
			.build();

		doTest(
			factory2.resolve(new TypeT<>() {}),
			new TestRecord2<>(1, new TestRecord1(456, "def")));
	}

	private static <T> void doTest(JsonCodec<T> codec, T expected) throws MalformedDataException {
		String json = JsonUtils.toJson(codec, expected);
		System.out.println(json);
		Assert.assertEquals(expected, JsonUtils.fromJson(codec, json));
	}
}
