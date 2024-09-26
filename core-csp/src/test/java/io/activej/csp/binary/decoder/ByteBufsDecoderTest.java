package io.activej.csp.binary.decoder;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
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

	private final ByteBufs bufs = new ByteBufs();
	private final ByteBufs tempBufs = new ByteBufs();

	@Test
	public void testOfNullTerminatedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoders.ofNullTerminatedBytes();
		bufs.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 0, 4, 5, 6}));
		ByteBuf beforeNull = doDecode(decoder);
		ByteBuf afterNull = bufs.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeNull.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterNull.asArray());

		bufs.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		beforeNull = doDecode(decoder);
		afterNull = bufs.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeNull.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterNull.asArray());
	}

	@Test
	public void ofCrlfTerminatedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoders.ofCrlfTerminatedBytes();
		bufs.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, CR, LF, 4, 5, 6}));
		ByteBuf beforeCrlf = doDecode(decoder);
		ByteBuf afterCrlf = bufs.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterCrlf.asArray());

		bufs.add(ByteBuf.wrapForReading(new byte[]{CR, LF, 1, 2, 3}));
		beforeCrlf = doDecode(decoder);
		afterCrlf = bufs.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterCrlf.asArray());
	}

	@Test
	public void ofCrlfTerminatedBytesWithMaxSize() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoders.ofCrlfTerminatedBytes(5);
		bufs.add(ByteBuf.wrapForReading(new byte[]{1, 2, CR, LF, 3, 4}));
		ByteBuf beforeCrlf = doDecode(decoder);
		ByteBuf afterCrlf = bufs.takeRemaining();

		assertArrayEquals(new byte[]{1, 2}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{3, 4}, afterCrlf.asArray());

		bufs.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, CR, LF, 4}));
		beforeCrlf = doDecode(decoder);
		afterCrlf = bufs.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeCrlf.asArray());
		assertArrayEquals(new byte[]{4}, afterCrlf.asArray());

		bufs.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4, CR, LF}));
		MalformedDataException e = assertThrows(MalformedDataException.class, () -> doDecode(decoder));
		assertEquals("No CRLF is found in 5 bytes", e.getMessage());
	}

	@Test
	public void ofIntSizePrefixedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoders.ofIntSizePrefixedBytes();
		ByteBuf buf = ByteBufPool.allocate(4);
		buf.writeInt(5);
		bufs.add(buf);
		byte[] bytes = {1, 2, 3, 4, 5};
		bufs.add(ByteBuf.wrapForReading(bytes));

		ByteBuf decoded = doDecode(decoder);
		assertNotNull(decoded);
		assertArrayEquals(bytes, decoded.asArray());
	}

	@Test
	public void ofVarIntSizePrefixedBytes() throws MalformedDataException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoders.ofVarIntSizePrefixedBytes();
		ByteBuf buf = ByteBufPool.allocate(1005);
		buf.writeVarInt(1000);
		bufs.add(buf);
		byte[] bytes = new byte[1000];
		ThreadLocalRandom.current().nextBytes(bytes);
		bufs.add(ByteBuf.wrapForReading(bytes));

		ByteBuf decoded = doDecode(decoder);
		assertNotNull(decoded);
		assertArrayEquals(bytes, decoded.asArray());
	}

	@Test
	public void assertBytes() throws MalformedDataException {
		byte[] bytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		byte[] otherBytes = {11, 12, 13};
		bufs.add(ByteBuf.wrapForReading(bytes));
		bufs.add(ByteBuf.wrapForReading(otherBytes));

		ByteBufsDecoder<byte[]> decoder = ByteBufsDecoders.assertBytes(bytes);
		byte[] decoded = doDecode(decoder);
		assertArrayEquals(bytes, decoded);
		assertArrayEquals(otherBytes, bufs.takeRemaining().asArray());

		bufs.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4, 6, 7, 8, 9, 10, 11}));

		MalformedDataException e = assertThrows(MalformedDataException.class, () -> doDecode(decoder));
		assertEquals(
			"Array of bytes differs at index " + 4 +
			"[Expected: " + 5 + ", actual: " + 6 + ']',
			e.getMessage());
	}

	private <T> T doDecode(ByteBufsDecoder<T> decoder) throws MalformedDataException {
		while (true) {
			T result = decoder.tryDecode(tempBufs);
			if (result != null) {
				assertTrue(tempBufs.isEmpty());
				return result;
			}
			tempBufs.add(bufs.takeExactSize(1));
		}
	}
}
