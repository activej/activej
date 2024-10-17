package io.activej.bytebuf;

import io.activej.common.concurrent.ObjectPool;
import org.junit.Test;

import static io.activej.bytebuf.ByteBufTest.initByteBufPool;
import static org.junit.Assert.*;

public class ByteArraySlabPoolTest {
	static {
		initByteBufPool();
	}

	private void checkAllocate(int size, int expectedSize, int[] poolSizes) {
		ByteBufPool.clear();

		ByteBuf bytes = ByteBufPool.allocate(size);
		assertEquals(expectedSize, bytes.array().length);
		for (int i = 0; i < poolSizes.length; i++) {
			assertTrue(ByteBufPool.slabs[i].isEmpty());
		}
		bytes.recycle();

		for (int i = 0; i < poolSizes.length; i++) {
			ObjectPool<ByteBuf> slab = ByteBufPool.slabs[i];
			assertEquals(poolSizes[i] == 0 ? 0 : 1, slab.size());
			ByteBuf polled = slab.poll();
			if (polled != null) {
				assertEquals(poolSizes[i], polled.limit());
				while ((polled = slab.poll()) != null) {
					assertTrue(polled.isRecycled());
				}
			}
		}
	}

	@Test
	public void testAllocate() {
		ByteBufPool.clear();

		checkAllocate(0, 0, new int[]{});
		checkAllocate(1, 1, new int[]{1, 0, 0, 0, 0});
		checkAllocate(2, 2, new int[]{0, 2, 0, 0, 0});
		checkAllocate(8, 8, new int[]{0, 0, 0, 8, 0});
		checkAllocate(9, 16, new int[]{0, 0, 0, 0, 16});
		checkAllocate(15, 16, new int[]{0, 0, 0, 0, 16});
	}

	private void checkReallocate(int size1, int size2, boolean equals) {
		ByteBufPool.clear();

		ByteBuf bytes1 = ByteBufPool.allocate(size1);
		assertTrue(size1 <= bytes1.limit());

		ByteBuf bytes2 = ByteBufPool.ensureWriteRemaining(bytes1, size2);
		assertTrue(size2 <= bytes2.limit());

		assertEquals(equals, bytes1 == bytes2);
	}

	@Test
	public void testReallocate() {
		ByteBufPool.clear();

		checkReallocate(0, 0, true);
		checkReallocate(0, 1, false);
		checkReallocate(0, 2, false);

		checkReallocate(1, 0, true);
		checkReallocate(1, 1, true);
		checkReallocate(1, 2, false);

		checkReallocate(2, 0, true);
		checkReallocate(2, 1, true);
		checkReallocate(2, 2, true);
		checkReallocate(2, 3, false);

		checkReallocate(15, 14, true);
		checkReallocate(15, 15, true);
		checkReallocate(15, 16, true);
		checkReallocate(15, 17, false);

		checkReallocate(16, 15, true);
		checkReallocate(16, 16, true);
		checkReallocate(16, 17, false);
		checkReallocate(16, 31, false);
		checkReallocate(16, 32, false);

		checkReallocate(31, 30, true);
		checkReallocate(31, 31, true);
	}

	private void checkReuse(int size) {
		ByteBuf zeroSized = ByteBufPool.allocate(size);
		zeroSized.recycle();
		ByteBuf reallocated = ByteBufPool.allocate(size);
		assertSame(reallocated, zeroSized);
		reallocated.recycle();
	}

	@Test
	public void testBufsReuse() {
		for (int i = 0; i < 100; i++) {
			checkReuse(i);
		}
	}
}
