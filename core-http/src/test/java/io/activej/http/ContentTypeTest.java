package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.http.HttpUtils.parseQ;
import static io.activej.http.MediaTypes.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ContentTypeTest {
	@Test
	public void testMediaType() {
		byte[] mediaType = encodeAscii("application/json");
		int hash = hashCodeLowerCaseAscii(mediaType);
		MediaType actual = MediaTypes.of(mediaType, 0, mediaType.length, hash);
		assertSame(JSON, actual);
	}

	@Test
	public void testContentTypeParse() throws MalformedHttpException {
		byte[] contentType = encodeAscii("text/plain;param=value; url-form=www;CHARSET=UTF-8; a=v");
		ContentType actual = ContentType.parse(contentType, 0, contentType.length);
		assertSame(MediaTypes.PLAIN_TEXT, actual.getMediaType());
		assertSame(UTF_8, actual.getCharset());
	}

	@Test
	public void testQParser() throws MalformedHttpException {
		byte[] num = encodeAscii("0.12313");
		int q = parseQ(num, 0, num.length);
		assertEquals(12, q);

		num = encodeAscii("1.0");
		q = parseQ(num, 0, num.length);
		assertEquals(100, q);

		num = encodeAscii("1");
		q = parseQ(num, 0, num.length);
		assertEquals(100, q);

		num = encodeAscii("0");
		q = parseQ(num, 0, num.length);
		assertEquals(0, q);
	}

	@Test
	public void testAcceptContentType() throws MalformedHttpException {
		byte[] acceptCts = encodeAscii("text/html;q=0.1, " +
				"application/xhtml+xml; method=get; q=0.3; bool=true," +
				"application/xml;q=0.9," +
				"image/webp," +
				"*/*;q=0.8," +
				"unknown/mime");
		List<AcceptMediaType> result = new ArrayList<>();
		AcceptMediaType.parse(acceptCts, 0, acceptCts.length, result);
		List<AcceptMediaType> expected = new ArrayList<>();
		expected.add(AcceptMediaType.of(HTML, 10));
		expected.add(AcceptMediaType.of(XHTML_APP, 30));
		expected.add(AcceptMediaType.of(XML_APP, 90));
		expected.add(AcceptMediaType.of(WEBP));
		expected.add(AcceptMediaType.of(ANY, 80));
		expected.add(AcceptMediaType.of(MediaType.of("unknown/mime")));
		assertEquals(expected.toString(), result.toString());
	}

	@Test
	public void testRenderMime() {
		String expected = "application/json";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		MediaTypes.render(JSON, buf);
		String actual = asAscii(buf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderContentType() {
		String expected = "text/html; charset=utf-8";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		ContentType type = ContentType.of(HTML, UTF_8);
		ContentType.render(type, buf);
		String actual = asAscii(buf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderAcceptContentType() {
		String expected = "text/html, application/xhtml+xml, application/xml; q=0.9, image/webp, */*; q=0.8";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		List<AcceptMediaType> acts = new ArrayList<>();
		acts.add(AcceptMediaType.of(HTML));
		acts.add(AcceptMediaType.of(XHTML_APP));
		acts.add(AcceptMediaType.of(MediaTypes.XML_APP, 90));
		acts.add(AcceptMediaType.of(WEBP));
		acts.add(AcceptMediaType.of(MediaTypes.ANY, 80));
		AcceptMediaType.render(acts, buf);
		String actual = asAscii(buf);
		assertEquals(expected, actual);
	}
}
