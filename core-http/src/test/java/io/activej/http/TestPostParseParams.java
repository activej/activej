package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static org.junit.Assert.assertEquals;

public final class TestPostParseParams {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testParameters() {
		ByteBuf body = wrapAscii("hello=world&value=1234");

		Map<String, String> params = UrlParser.parseQueryIntoMap(decodeAscii(body.array(), body.head(), body.readRemaining()));

		assertEquals(2, params.size());
		assertEquals("world", params.get("hello"));
		assertEquals("1234", params.get("value"));

		body.recycle();
	}
}
