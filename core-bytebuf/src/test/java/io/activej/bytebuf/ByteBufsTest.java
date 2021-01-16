package io.activej.bytebuf;

import io.activej.common.exception.MalformedDataException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.bytebuf.ByteBufTest.initByteBufPool;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteBufsTest {
	static {
		initByteBufPool();
	}

	private final Random random = new Random();
	private ByteBufs bufs;

	@Before
	public void setUp() {
		bufs = new ByteBufs();
	}

	@After
	public void tearDown() {
		bufs.recycle();
	}

	@Test
	public void test() {
		byte[] test = new byte[200];
		for (int i = 0; i < test.length; i++) {
			test[i] = (byte) (i + 1);
		}

		int left = test.length;
		int pos = 0;
		while (left > 0) {
			int bufSize = random.nextInt(Math.min(10, left) + 1);
			ByteBuf buf = ByteBuf.wrap(test, pos, pos + bufSize);
			bufs.add(buf);
			left -= bufSize;
			pos += bufSize;
		}

		assertEquals(test.length, bufs.remainingBytes());

		left = test.length;
		pos = 0;
		while (left > 0) {
			int requested = random.nextInt(50);
			byte[] dest = new byte[100];
			int drained = bufs.drainTo(dest, 10, requested);

			assertTrue(drained <= requested);

			for (int i = 0; i < drained; i++) {
				assertEquals(test[i + pos], dest[i + 10]);
			}

			left -= drained;
			pos += drained;
		}

		assertEquals(0, bufs.remainingBytes());
	}

	@Test
	public void testAsIterator() {
		List<ByteBuf> expected = asList(wrapAscii("First"), wrapAscii("Second"), wrapAscii("Third"), wrapAscii("Fourth"));
		bufs.addAll(expected);

		List<ByteBuf> actual = new ArrayList<>();
		bufs.asIterator().forEachRemaining(actual::add);
		assertEquals(expected, actual);
	}

	@Test
	public void scanEmptyBufs() throws MalformedDataException {
		assertTrue(bufs.isEmpty());
		assertEquals(0, bufs.scanBytes((index, value) -> {
			throw new AssertionError();
		}));
	}

	@Test
	public void scanOverBufsSize() throws MalformedDataException {
		bufs.add(ByteBuf.wrapForReading(new byte[5]));
		assertEquals(0, bufs.scanBytes(bufs.remainingBytes(), (index, value) -> {
			throw new AssertionError();
		}));
	}

	@Test
	public void scanWithOffsetOnBufBorder() throws MalformedDataException {
		byte[] bytes = {1, 2, 3, 4, 5};
		bufs.add(ByteBuf.wrapForReading(bytes));
		bufs.add(ByteBuf.wrapForReading(bytes));

		assertEquals(1, bufs.scanBytes(bytes.length, (index, value) -> {
			assertEquals(0, index);
			assertEquals(bytes[0], value);
			return true;
		}));
	}
}
