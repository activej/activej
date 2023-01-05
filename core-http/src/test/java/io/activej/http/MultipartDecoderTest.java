package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class MultipartDecoderTest {
	private static final String BOUNDARY = "--test-boundary-123";
	private static final String CRLF = "\r\n";

	private static final String DATA = BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"" + CRLF +
			"Content-Type: text/plain" + CRLF +
			CRLF +
			"This is some bytes of data to be extracted from the multipart form" + CRLF +
			"Also here we had a wild CRLF se\r\nquence appear" +
			CRLF + BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"" + CRLF +
			"Content-Type: text/plain" + CRLF +
			"Test-Extra-Header: one" + CRLF +
			"Test-Extra-Header-2: two" + CRLF +
			CRLF +
			"\nAnd the second " +
			CRLF + BOUNDARY + CRLF +
			CRLF +
			"line, huh\n" +
			CRLF + BOUNDARY + "--" + CRLF;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() {
		List<ByteBuf> split = new ArrayList<>();
		int i = 0;
		while (i < DATA.length() / 5 - 1) {
			split.add(ByteBuf.wrapForReading(DATA.substring(i * 5, ++i * 5).getBytes(UTF_8)));
		}
		if (DATA.length() != (i *= 5)) {
			split.add(ByteBuf.wrapForReading(DATA.substring(i).getBytes(UTF_8)));
		}

		List<Map<String, String>> headers = new ArrayList<>();

		String res = await(BinaryChannelSupplier.of(ChannelSupplier.ofList(split))
				.decodeStream(MultipartDecoder.create(BOUNDARY.substring(2)))
				.toCollector(mapping(frame -> {
					if (frame.isData()) {
						return frame.getData().asString(UTF_8);
					}
					assertFalse(frame.getHeaders().isEmpty());
					headers.add(frame.getHeaders());
					return "";
				}, joining())));

		assertEquals("""
				This is some bytes of data to be extracted from the multipart form\r
				Also here we had a wild CRLF se\r
				quence appear
				And the second line, huh
				""", res);
		assertEquals(List.of(
                Map.of(
						"content-disposition", "form-data; name=\"file\"; filename=\"test.txt\"",
						"content-type", "text/plain"),
				Map.of("content-disposition", "form-data; name=\"file\"; filename=\"test.txt\"",
						"content-type", "text/plain",
						"test-extra-header", "one",
						"test-extra-header-2", "two")
		), headers);
	}

	@Test
	public void testSplitOnlyLastPart() {
		// last boundary
		ByteBuf buf = ByteBufStrings.wrapUtf8(BOUNDARY + "--" + CRLF);
		MultipartDecoder decoder = MultipartDecoder.create(BOUNDARY.substring(2));

		await(decoder.split(ChannelSupplier.of(buf), new MultipartDecoder.AsyncMultipartDataHandler() {
			@Override
			public Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName) {
				return Promise.ofException(new ExpectedException());
			}

			@Override
			public Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName) {
				return Promise.ofException(new ExpectedException());
			}
		}));
	}
}
