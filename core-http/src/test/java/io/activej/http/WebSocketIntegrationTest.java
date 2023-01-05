package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelSupplier;
import io.activej.http.AsyncWebSocket.Frame;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.activej.bytebuf.ByteBuf.wrapForReading;
import static io.activej.csp.dsl.ChannelSupplierTransformer.identity;
import static io.activej.http.TestUtils.*;
import static io.activej.http.AsyncWebSocket.Frame.FrameType.BINARY;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

public final class WebSocketIntegrationTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final int MAX_MESSAGE_SIZE = 70_000;

	@Test
	public void simpleMessage() {
		String message = "Hello";
		doTest(message.getBytes(UTF_8));
	}

	@Test
	public void binaryData() {
		doTest(randomBytes(0));
		doTest(randomBytes(1));
		doTest(randomBytes(100));

		// edge cases
		doTest(randomBytes(125));
		doTest(randomBytes(126));
		doTest(randomBytes(127));
		doTest(randomBytes(128));

		doTest(randomBytes(65535));
		doTest(randomBytes(65536));
		doTest(randomBytes(65537));
	}

	private void doTest(byte[] bytes) {
		doTest(bytes, false, false);
		doTest(bytes, false, true);
		doTest(bytes, true, false);
		doTest(bytes, true, true);
	}

	private void doTest(byte[] bytes, boolean mask, boolean chunked) {
		byte[] bytesCopy = Arrays.copyOf(bytes, bytes.length);
		ByteBuf buf = wrapForReading(bytesCopy);
		List<Frame> result = await(ChannelSupplier.of(Frame.binary(buf))
				.transformWith(WebSocketFramesToBufs.create(mask))
				.transformWith(chunked ? chunker() : identity())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), failOnItem(), mask))
				.toCollector(toList()));

		assertEquals(1, result.size());
		Frame frame = result.get(0);
		assertArrayEquals(bytes, frame.getPayload().asArray());
		assertEquals(BINARY, frame.getType());
		assertTrue(frame.isLastFrame());
	}
}
