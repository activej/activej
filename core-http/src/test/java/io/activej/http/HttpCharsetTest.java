package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.asAscii;
import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static java.nio.charset.Charset.forName;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class HttpCharsetTest {
	@Test
	public void testCharsetRender() {
		String expected = "utf-8";
		HttpCharset charset = HttpCharset.UTF_8;
		byte[] container = new byte[5];
		int pos = HttpCharset.render(charset, container, 0);
		assertEquals(5, pos);
		assertEquals(expected, new String(container));
	}

	@Test
	public void testParser() {
		HttpCharset expected = HttpCharset.US_ASCII;
		byte[] asciis = "us-ascii".getBytes(US_ASCII);
		HttpCharset actual = HttpCharset.parse(asciis, 0, asciis.length);
		assertSame(expected, actual);
	}

	@Test
	public void testConverters() throws HttpParseException {
		HttpCharset expected = HttpCharset.US_ASCII;
		Charset charset = expected.toJavaCharset();
		HttpCharset actual = HttpCharset.of(charset);
		assertSame(expected, actual);
	}

	@Test
	public void testAcceptCharset() throws HttpParseException {
		byte[] bytes = encodeAscii("iso-8859-5, unicode-1-1;q=0.8");
		List<AcceptCharset> chs = new ArrayList<>();
		AcceptCharset.parse(bytes, 0, bytes.length, chs);
		assertEquals(2, chs.size());
		assertSame(forName("ISO-8859-5"), chs.get(0).getCharset());
		assertEquals(80, chs.get(1).getQ());
	}

	@Test
	public void testRenderAcceptCharset() {
		String expected = "iso-8859-1, UTF-16; q=0.8";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		List<AcceptCharset> chs = new ArrayList<>();
		chs.add(AcceptCharset.of(StandardCharsets.ISO_8859_1));
		chs.add(AcceptCharset.of(StandardCharsets.UTF_16, 80));
		AcceptCharset.render(chs, buf);
		String actual = asAscii(buf);
		assertEquals(expected, actual);
	}
}
