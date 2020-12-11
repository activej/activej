package io.activej.codec;

import io.activej.codec.json.JsonUtils;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.tuple.Tuple2;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.codec.StructuredCodecs.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"ConstantConditions", "ArraysAsListWithZeroOrOneArgument"})
public class StructuredCodecTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private <T> void test(StructuredCodec<T> codec, T item) throws MalformedDataException {
		String str = JsonUtils.toJson(codec, item);
		System.out.println(str);
		T result = JsonUtils.fromJson(codec, str);
		assertEquals(item, result);
	}

	@Test
	public void test1() throws MalformedDataException {
		StructuredCodec<Tuple2<String, Integer>> codec = tuple(Tuple2::new,
				Tuple2::getValue1, STRING_CODEC.nullable(),
				Tuple2::getValue2, INT_CODEC.nullable());

		test(STRING_CODEC.ofList(), asList("abc"));

		test(codec.ofList().nullable(), null);
		test(codec.nullable().ofList(), asList((Tuple2<String, Integer>) null));
		test(codec.ofList(), asList(new Tuple2<>("abc", 123)));
		test(codec.nullable().ofList(), asList(null, new Tuple2<>("abc", 123)));

		test(STRING_CODEC, "abc");
		test(STRING_CODEC.nullable(), null);

		test(codec, new Tuple2<>("abc", 123));
		test(codec, new Tuple2<>(null, 123));
		test(codec, new Tuple2<>(null, null));

		test(codec.nullable(), null);
	}
}
