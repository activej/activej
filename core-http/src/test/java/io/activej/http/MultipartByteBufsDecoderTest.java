package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.supplier.ChannelSuppliers;
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

public final class MultipartByteBufsDecoderTest {
	private static final String BOUNDARY = "--test-boundary-123";
	private static final String CRLF = "\r\n";

	private static final String DATA = """
		$boundary\r
		Content-Disposition: form-data; name="file"; filename="test.txt"\r
		Content-Type: text/plain\r
		\r
		This is some bytes of data to be extracted from the multipart form\r
		Also here we had a wild CRLF se\r
		quence appear\r
		$boundary\r
		Content-Disposition: form-data; name="file"; filename="test.txt"\r
		Content-Type: text/plain\r
		Test-Extra-Header: one\r
		Test-Extra-Header-2: two\r
		\r

		And the second \r
		$boundary\r
		\r
		line, huh
		\r
		$boundary--"""
		.replace("$boundary", BOUNDARY);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		doTest(DATA);
	}

	@Test
	public void testWithLastCRLF() {
		doTest(DATA + CRLF);
	}

	@SuppressWarnings("ConstantConditions")
	private static void doTest(String data) {
		List<ByteBuf> split = new ArrayList<>();
		int i = 0;
		while (i < data.length() / 5 - 1) {
			split.add(ByteBuf.wrapForReading(data.substring(i * 5, ++i * 5).getBytes(UTF_8)));
		}
		if (data.length() != (i *= 5)) {
			split.add(ByteBuf.wrapForReading(data.substring(i).getBytes(UTF_8)));
		}

		List<Map<String, String>> headers = new ArrayList<>();

		String res = await(BinaryChannelSupplier.of(ChannelSuppliers.ofList(split))
			.decodeStream(MultipartByteBufsDecoder.create(BOUNDARY.substring(2)))
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
		MultipartByteBufsDecoder decoder = MultipartByteBufsDecoder.create(BOUNDARY.substring(2));

		await(decoder.split(ChannelSuppliers.ofValue(buf), new MultipartByteBufsDecoder.AsyncMultipartDataHandler() {
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
