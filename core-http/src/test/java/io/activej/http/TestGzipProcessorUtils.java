package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.http.GzipProcessorUtils.fromGzip;
import static io.activej.http.GzipProcessorUtils.toGzip;
import static io.activej.http.HttpHeaders.ACCEPT_ENCODING;
import static io.activej.http.HttpHeaders.CONTENT_ENCODING;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

@RunWith(Parameterized.class)
public final class TestGzipProcessorUtils {
	public static final int CHARACTERS_COUNT = 10_000_000;

	private int port;

	@Parameters
	public static List<String> testData() {
		return List.of(
				"",
				"I grant! I've never seen a goddess go. My mistress, when she walks, treads on the ground",
				generateLargeText()
		);
	}

	@Parameter
	public String text;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Before
	public void setUp() {
		port = getFreePort();
	}

	@Test
	public void testEncodeDecode() throws MalformedHttpException {
		ByteBuf raw = toGzip(wrapUtf8(text));
		ByteBuf actual = fromGzip(raw, 11_000_000);
		assertEquals(text, actual.asString(UTF_8));
	}

	@Test
	public void testEncodeDecodeWithTrailerInputSizeLessThenActual() {
		ByteBuf raw = toGzip(wrapUtf8(text));
		raw.moveTail(-4);
		raw.writeInt(Integer.reverseBytes(2));

		try {
			fromGzip(raw, 11_000_000);
			fail();
		} catch (MalformedHttpException e) {
			assertEquals("Decompressed data size is not equal to input size from GZIP trailer", e.getMessage());
		}
	}

	@Test
	public void recycleByteBufInCaseOfBadInput() {
		ByteBuf badBuf = ByteBufPool.allocate(100);
		badBuf.put(new byte[]{-1, -1, -1, -1, -1, -1});

		try {
			fromGzip(badBuf, 11_000_000);
			fail();
		} catch (MalformedHttpException e) {
			assertEquals("Corrupted GZIP header", e.getMessage());
		}
	}

	@Test
	public void testGzippedCommunicationBetweenClientServer() throws IOException {
		HttpServer server = HttpServer.create(Reactor.getCurrentReactor(),
						request -> request.loadBody(CHARACTERS_COUNT)
								.map(body -> {
									assertEquals("gzip", request.getHeader(CONTENT_ENCODING));
									assertEquals("gzip", request.getHeader(ACCEPT_ENCODING));

									String receivedData = body.getString(UTF_8);
									assertEquals(text, receivedData);
									return HttpResponse.ok200()
											.withBodyGzipCompression()
											.withBody(ByteBufStrings.wrapAscii(receivedData));
								}))
				.withListenPort(port);

		AsyncHttpClient client = ReactiveHttpClient.create(Reactor.getCurrentReactor());

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + port)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBodyGzipCompression()
				.withBody(wrapUtf8(text));

		server.listen();

		ByteBuf body = await(client.request(request)
				.whenComplete(assertCompleteFn(response -> assertEquals("gzip", response.getHeader(CONTENT_ENCODING))))
				.then(response -> response.loadBody(CHARACTERS_COUNT))
				.map(buf -> buf.slice())
				.whenComplete(server::close));

		assertEquals(text, body.asString(UTF_8));
	}

	@Test
	public void testGzipInputStreamCorrectlyDecodesDataEncoded() throws IOException {
		ByteBuf encodedData = toGzip(wrapUtf8(text));
		ByteBuf decoded = decodeWithGzipInputStream(encodedData);
		assertEquals(text, decoded.asString(UTF_8));
	}

	@Test
	public void testGzipOutputStreamDataIsCorrectlyDecoded() throws IOException, MalformedHttpException {
		ByteBuf encodedData = encodeWithGzipOutputStream(wrapUtf8(text));
		ByteBuf decoded = fromGzip(encodedData, 11_000_000);
		assertEquals(text, decoded.asString(UTF_8));
	}

	private static ByteBuf encodeWithGzipOutputStream(ByteBuf raw) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
			gzip.write(raw.array(), raw.head(), raw.readRemaining());
			gzip.finish();
			byte[] bytes = baos.toByteArray();
			ByteBuf byteBuf = ByteBufPool.allocate(bytes.length);
			byteBuf.put(bytes);
			return byteBuf;
		} finally {
			raw.recycle();
		}
	}

	private static ByteBuf decodeWithGzipInputStream(ByteBuf src) throws IOException {
		try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(src.array(), src.head(), src.readRemaining()))) {
			int nRead;
			ByteBuf data = ByteBufPool.allocate(256);
			while ((nRead = gzip.read(data.array(), data.tail(), data.writeRemaining())) != -1) {
				data.moveTail(nRead);
				data = ByteBufPool.ensureWriteRemaining(data, data.readRemaining());
			}
			src.recycle();
			return data;
		}
	}

	private static String generateLargeText() {
		Random charRandom = new Random(1L);
		int charactersCount = CHARACTERS_COUNT;
		StringBuilder sb = new StringBuilder(charactersCount);
		for (int i = 0; i < charactersCount; i++) {
			int charCode = charRandom.nextInt(127);
			sb.append((char) charCode);
		}
		return sb.toString();
	}
}
