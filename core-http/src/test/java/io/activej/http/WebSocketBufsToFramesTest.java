package io.activej.http;

import io.activej.bytebuf.ByteBufs;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.http.AsyncWebSocket.Frame;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.bytebuf.ByteBuf.wrapForReading;
import static io.activej.http.AsyncWebSocket.Frame.FrameType.*;
import static io.activej.http.TestUtils.*;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public final class WebSocketBufsToFramesTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final int MAX_MESSAGE_SIZE = 100_000;

	@Test
	public void decodeSingleFrameUnmaskedTextMessage() {
		// Unmasked "Hello" message, RFC 6455 - 5.7
		byte[] frame = new byte[]{(byte) 0x81, (byte) 0x05, (byte) 0x48, (byte) 0x65, (byte) 0x6c, (byte) 0x6c, (byte) 0x6f};

		Frame result = await(ChannelSupplier.of(wrapForReading(frame))
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), failOnItem(), false))
				.get());

		assertEquals(TEXT, result.getType());
		assertEquals("Hello", result.getPayload().asString(UTF_8));
		assertTrue(result.isLastFrame());
	}

	@Test
	public void decodeSingleFrameMaskedTextMessage() {
		// Masked "Hello" message, RFC 6455 - 5.7
		byte[] frame = new byte[]{(byte) 0x81, (byte) 0x85, (byte) 0x37, (byte) 0xfa, (byte) 0x21,
				(byte) 0x3d, (byte) 0x7f, (byte) 0x9f, (byte) 0x4d, (byte) 0x51, (byte) 0x58};

		Frame result = await(ChannelSupplier.of(wrapForReading(frame))
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), failOnItem(), true))
				.get());

		assertEquals(TEXT, result.getType());
		assertEquals("Hello", result.getPayload().asString(UTF_8));
		assertTrue(result.isLastFrame());
	}

	@Test
	public void decodeFragmentedUnmaskedTextMessage() {
		// Contains "Hel" message, RFC 6455 - 5.7
		byte[] frame1 = new byte[]{(byte) 0x01, (byte) 0x03, (byte) 0x48, (byte) 0x65, (byte) 0x6c};
		// Contains "lo" message, RFC 6455 - 5.7
		byte[] frame2 = new byte[]{(byte) 0x80, (byte) 0x02, (byte) 0x6c, (byte) 0x6f};

		ChannelSupplier<Frame> supplier = ChannelSupplier.of(wrapForReading(frame1), wrapForReading(frame2))
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), failOnItem(), false));

		Frame firstFrame = await(supplier.get());
		assertEquals(TEXT, firstFrame.getType());
		assertEquals("Hel", firstFrame.getPayload().asString(UTF_8));
		assertFalse(firstFrame.isLastFrame());


		Frame secondFrame = await(supplier.get());
		assertEquals(CONTINUATION, secondFrame.getType());
		assertEquals("lo", secondFrame.getPayload().asString(UTF_8));
		assertTrue(secondFrame.isLastFrame());
	}

	@Test
	public void decode256BytesInSingleUnmaskedFrame() {
		// RFC 6455 - 5.7
		byte[] header = new byte[]{(byte) 0x82, (byte) 0x7E, (byte) 0x01, (byte) 0x00};
		byte[] payload = randomBytes(256);

		Frame result = await(ChannelSupplier.of(wrapForReading(header), wrapForReading(payload))
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), failOnItem(), false))
				.get());

		assertEquals(BINARY, result.getType());
		assertArrayEquals(payload, result.getPayload().asArray());
		assertTrue(result.isLastFrame());
	}

	@Test
	public void decode64KiBInSingleUnmaskedFrame() {
		// RFC 6455 - 5.7
		byte[] header = new byte[]{(byte) 0x82, (byte) 0x7F, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
				(byte) 0x00, (byte) 0x01, (byte) 0x00, (byte) 0x00};
		byte[] payload = randomBytes(65536);

		Frame result = await(ChannelSupplier.of(wrapForReading(header), wrapForReading(payload))
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), failOnItem(), false))
				.get());

		assertEquals(BINARY, result.getType());
		assertArrayEquals(payload, result.getPayload().asArray());
		assertTrue(result.isLastFrame());
	}

	@Test
	public void decodeUnmaskedPing() {
		// Unmasked Ping request, RFC 6455 - 5.7
		byte[] pingFrame = new byte[]{(byte) 0x89, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f};

		ByteBufs pingMessage = new ByteBufs();
		await(ChannelSupplier.of(wrapForReading(pingFrame), closeUnmasked())
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, pingMessage::add, failOnItem(), false))
				.streamTo(ChannelConsumer.ofConsumer($ -> fail())));

		assertEquals("Hello", pingMessage.takeRemaining().asString(UTF_8));
	}

	@Test
	public void decodeMaskedPong() {
		// Masked Ping response, RFC 6455 - 5.7
		byte[] pingFrame = new byte[]{(byte) 0x8a, (byte) 0x85, (byte) 0x37, (byte) 0xfa, (byte) 0x21,
				(byte) 0x3d, (byte) 0x7f, (byte) 0x9f, (byte) 0x4d, (byte) 0x51, (byte) 0x58};

		ByteBufs pongMessage = new ByteBufs();
		await(ChannelSupplier.of(wrapForReading(pingFrame), closeMasked())
				.transformWith(chunker())
				.transformWith(WebSocketBufsToFrames.create(MAX_MESSAGE_SIZE, failOnItem(), pongMessage::add, true))
				.streamTo(ChannelConsumer.ofConsumer($ -> fail())));

		assertEquals("Hello", pongMessage.takeRemaining().asString(UTF_8));
	}
}
