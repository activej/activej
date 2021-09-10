package io.activej.http.decoder;

import io.activej.common.collection.Either;
import io.activej.http.HttpCookie;
import io.activej.http.HttpRequest;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DecoderTest {
	@ClassRule
	public static final ByteBufRule rule = new ByteBufRule();

	@Test
	public void test() throws DecodeException {
		Decoder<String> parser = Decoders.ofCookie("tmp");
		assertEquals("1",
				parser.decodeOrThrow(HttpRequest.get("http://example.com")
						.withCookie(HttpCookie.of("tmp", "1"))));
	}

	@Test
	public void testMap() {
		Decoder<Double> parser = Decoders.ofCookie("key")
				.map(Mapper.of(Integer::parseInt))
				.validate(Validator.of(param -> param > 10, "Lower then 10"))
				.map(Mapper.of(Integer::doubleValue))
				.validate(Validator.of(value -> value % 2 == 0, "Is even"));

		Either<Double, DecodeErrors> key = parser.decode(HttpRequest.get("http://example.com")
				.withCookie(HttpCookie.of("key", "11")));
		DecodeErrors exception = key.getRight();
		//noinspection ConstantConditions
		assertEquals("Is even", exception.getErrors().get(0).getMessage());
	}
}
