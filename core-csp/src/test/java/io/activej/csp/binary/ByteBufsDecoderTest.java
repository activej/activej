package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.parse.ParseException;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

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
}
