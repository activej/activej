package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.ParseException;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static org.junit.Assert.*;

public final class ByteBufsDecoderTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public final ByteBufQueue queue = new ByteBufQueue();

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testOfNullTerminatedBytes() throws ParseException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofNullTerminatedBytes();
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 0, 4, 5, 6}));
		ByteBuf beforeNull = decoder.tryDecode(queue);
		ByteBuf afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeNull.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterNull.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		beforeNull = decoder.tryDecode(queue);
		afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeNull.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterNull.asArray());
	}

	@Test
	@SuppressWarnings("ConstantConditions")
	public void ofCrlfTerminatedBytes() throws ParseException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofCrlfTerminatedBytes();
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, CR, LF, 4, 5, 6}));
		ByteBuf beforeCrlf = decoder.tryDecode(queue);
		ByteBuf afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterCrlf.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{CR, LF, 1, 2, 3}));
		beforeCrlf = decoder.tryDecode(queue);
		afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterCrlf.asArray());
	}

	@Test
	@SuppressWarnings("ConstantConditions")
	public void ofCrlfTerminatedBytesWithMaxSize() throws ParseException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofCrlfTerminatedBytes(5);
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, CR, LF, 3, 4}));
		ByteBuf beforeCrlf = decoder.tryDecode(queue);
		ByteBuf afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{3, 4}, afterCrlf.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, CR, LF, 4}));
		beforeCrlf = decoder.tryDecode(queue);
		afterCrlf = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{4}, afterCrlf.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4, CR, LF}));
		try {
			decoder.tryDecode(queue);
			fail();
		} catch (ParseException e) {
			assertEquals("No CRLF is found in 5 bytes", e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("ConstantConditions")
	public void ofIntSizePrefixedBytes() throws ParseException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofIntSizePrefixedBytes();
		ByteBuf buf = ByteBufPool.allocate(4);
		buf.writeInt(5);
		queue.add(buf);
		byte[] bytes = {1, 2, 3, 4, 5};
		queue.add(ByteBuf.wrapForReading(bytes));

		ByteBuf decoded = decoder.tryDecode(queue);
		assertArrayEquals(bytes, decoded.asArray());
	}

	@Test
	public void assertBytes() throws ParseException {
		byte[] bytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		byte[] otherBytes = {11, 12, 13};
		queue.add(ByteBuf.wrapForReading(bytes));
		queue.add(ByteBuf.wrapForReading(otherBytes));

		ByteBufsDecoder<byte[]> decoder = ByteBufsDecoder.assertBytes(bytes);
		byte[] decoded = decoder.tryDecode(queue);
		assertArrayEquals(bytes, decoded);
		assertArrayEquals(otherBytes, queue.takeRemaining().asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4, 6, 7, 8, 9, 10, 11}));
		try {
			decoder.tryDecode(queue);
			fail();
		} catch (ParseException e){
			assertEquals("Array of bytes differs at index " + 4 +
					"[Expected: " + 5 + ", actual: " + 6 + ']', e.getMessage());
		}
	}
}
