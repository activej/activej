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

public class ByteBufQueueTest {
	static {
		initByteBufPool();
	}

	private final Random random = new Random();
	private ByteBufQueue queue;

	@Before
	public void setUp() {
		queue = new ByteBufQueue();
	}

	@After
	public void tearDown() {
		queue.recycle();
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
			queue.add(buf);
			left -= bufSize;
			pos += bufSize;
		}

		assertEquals(test.length, queue.remainingBytes());

		left = test.length;
		pos = 0;
		while (left > 0) {
			int requested = random.nextInt(50);
			byte[] dest = new byte[100];
			int drained = queue.drainTo(dest, 10, requested);

			assertTrue(drained <= requested);

			for (int i = 0; i < drained; i++) {
				assertEquals(test[i + pos], dest[i + 10]);
			}

			left -= drained;
			pos += drained;
		}

		assertEquals(0, queue.remainingBytes());
	}

	@Test
	public void testAsIterator() {
		List<ByteBuf> expected = asList(wrapAscii("First"), wrapAscii("Second"), wrapAscii("Third"), wrapAscii("Fourth"));
		queue.addAll(expected);

		List<ByteBuf> actual = new ArrayList<>();
		queue.asIterator().forEachRemaining(actual::add);
		assertEquals(expected, actual);
	}

	@Test
	public void scanEmptyQueue() throws MalformedDataException {
		assertTrue(queue.isEmpty());
		assertEquals(-1, queue.scanBytes((index, value) -> {
			throw new AssertionError();
		}));
	}

	@Test
	public void scanOverQueueSize() throws MalformedDataException {
		queue.add(ByteBuf.wrapForReading(new byte[5]));
		assertEquals(-1, queue.scanBytes(queue.remainingBytes(), (index, value) -> {
			throw new AssertionError();
		}));
	}

	@Test
	public void scanWithOffsetOnBufBorder() throws MalformedDataException {
		byte[] bytes = {1, 2, 3, 4, 5};
		queue.add(ByteBuf.wrapForReading(bytes));
		queue.add(ByteBuf.wrapForReading(bytes));

		int expectedIndex = bytes.length;
		assertEquals(expectedIndex, queue.scanBytes(bytes.length, (index, value) ->{
			assertEquals(expectedIndex, index);
			assertEquals(bytes[0], value);
			return true;
		}));
	}
}
