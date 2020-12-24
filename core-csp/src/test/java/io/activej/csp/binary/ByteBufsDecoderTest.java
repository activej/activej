package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.MalformedDataException;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static org.junit.Assert.*;

public final class ByteBufsDecoderTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final ByteBufQueue queue = new ByteBufQueue();
	private final ByteBufQueue tempQueue = new ByteBufQueue();

	@Test
	public void testOfNullTerminatedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofNullTerminatedBytes();
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 0, 4, 5, 6}));
		ByteBuf beforeNull = doDecode(decoder);
		ByteBuf afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeNull.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterNull.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		beforeNull = doDecode(decoder);
		afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeNull.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterNull.asArray());
	}

	@Test
	public void ofCrlfTerminatedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofCrlfTerminatedBytes();
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, CR, LF, 4, 5, 6}));
		ByteBuf beforeCrlf = doDecode(decoder);
		ByteBuf afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterCrlf.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{CR, LF, 1, 2, 3}));
		beforeCrlf = doDecode(decoder);
		afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterCrlf.asArray());
	}

	@Test
	public void ofCrlfTerminatedBytesWithMaxSize() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofCrlfTerminatedBytes(5);
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, CR, LF, 3, 4}));
		ByteBuf beforeCrlf = doDecode(decoder);
		ByteBuf afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{3, 4}, afterCrlf.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, CR, LF, 4}));
		beforeCrlf = doDecode(decoder);
		afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{4}, afterCrlf.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4, CR, LF}));
		try {
			doDecode(decoder);
			fail();
		} catch (MalformedDataException e) {
			assertEquals("No CRLF is found in 5 bytes", e.getMessage());
		}
	}

	@Test
	public void ofIntSizePrefixedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofIntSizePrefixedBytes();
		ByteBuf buf = ByteBufPool.allocate(4);
		buf.writeInt(5);
		queue.add(buf);
		byte[] bytes = {1, 2, 3, 4, 5};
		queue.add(ByteBuf.wrapForReading(bytes));

		ByteBuf decoded = doDecode(decoder);
		assertNotNull(decoded);
		assertArrayEquals(bytes, decoded.asArray());
	}

	@Test
	public void ofVarIntSizePrefixedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofVarIntSizePrefixedBytes();
		ByteBuf buf = ByteBufPool.allocate(1005);
		buf.writeVarInt(1000);
		queue.add(buf);
		byte[] bytes = new byte[1000];
		ThreadLocalRandom.current().nextBytes(bytes);
		queue.add(ByteBuf.wrapForReading(bytes));

		ByteBuf decoded = doDecode(decoder);
		assertNotNull(decoded);
		assertArrayEquals(bytes, decoded.asArray());
	}

	@Test
	public void assertBytes() throws MalformedDataException {
		byte[] bytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		byte[] otherBytes = {11, 12, 13};
		queue.add(ByteBuf.wrapForReading(bytes));
		queue.add(ByteBuf.wrapForReading(otherBytes));

		ByteBufsDecoder<byte[]> decoder = ByteBufsDecoder.assertBytes(bytes);
		byte[] decoded = doDecode(decoder);
		assertArrayEquals(bytes, decoded);
		assertArrayEquals(otherBytes, queue.takeRemaining().asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4, 6, 7, 8, 9, 10, 11}));
		try {
			doDecode(decoder);
			fail();
		} catch (MalformedDataException e) {
			assertEquals("Array of bytes differs at index " + 4 +
					"[Expected: " + 5 + ", actual: " + 6 + ']', e.getMessage());
		}
	}

	private <T> T doDecode(ByteBufsDecoder<T> decoder) throws MalformedDataException {
		while (true) {
			T result = decoder.tryDecode(tempQueue);
			if (result != null) {
				assertTrue(tempQueue.isEmpty());
				return result;
			}
			tempQueue.add(queue.takeExactSize(1));
		}
	}
}
