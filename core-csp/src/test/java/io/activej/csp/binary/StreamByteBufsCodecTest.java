package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamInput;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class StreamByteBufsCodecTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testCodec() throws MalformedDataException {
		ByteBufsCodec<String, String> codec = new StreamByteBufsCodec<>(
				StreamCodecs.ofString(),
				StreamCodecs.ofString()
		);

		ByteBufs bufs = new ByteBufs();
		bufs.add(codec.encode("one"));
		bufs.add(codec.encode("two"));
		bufs.add(codec.encode("three"));

		ByteBuf fourBuf = codec.encode("four");
		bufs.add(fourBuf.slice(0, 2));

		assertEquals("one", codec.tryDecode(bufs));
		assertEquals("two", codec.tryDecode(bufs));
		assertEquals("three", codec.tryDecode(bufs));
		assertNull(codec.tryDecode(bufs));

		bufs.add(fourBuf.slice(2, 3));
		assertEquals("four", codec.tryDecode(bufs));

		fourBuf.recycle();
	}

	@Test
	public void testCodecLargeMessages() throws MalformedDataException {
		ByteBufsCodec<byte[], byte[]> codec = ByteBufsCodec.ofStreamCodecs(
				StreamCodecs.ofByteArray(),
				StreamCodecs.ofByteArray()
		);

		Random random = ThreadLocalRandom.current();
		ByteBufs bufs = new ByteBufs();

		byte[] message1 = new byte[StreamInput.DEFAULT_BUFFER_SIZE * 4];
		byte[] message2 = new byte[message1.length];
		byte[] message3 = new byte[message1.length];
		random.nextBytes(message1);
		random.nextBytes(message2);
		random.nextBytes(message3);

		bufs.add(codec.encode(message1));
		bufs.add(codec.encode(message2));

		ByteBuf thirdBuf = codec.encode(message3);
		int readRemaining = thirdBuf.readRemaining();
		int half = readRemaining / 2;
		bufs.add(thirdBuf.slice(0, half));

		assertArrayEquals(message1, codec.tryDecode(bufs));
		assertArrayEquals(message2, codec.tryDecode(bufs));
		assertNull(codec.tryDecode(bufs));

		bufs.add(thirdBuf.slice(half, readRemaining - half));
		assertArrayEquals(message3, codec.tryDecode(bufs));

		thirdBuf.recycle();
	}
}
