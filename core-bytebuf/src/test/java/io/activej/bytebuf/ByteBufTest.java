package io.activej.bytebuf;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ByteBufTest {
	static {
		initByteBufPool();
	}

	public static void initByteBufPool() {
		System.setProperty("ByteBufPool.stats", "true");
		System.setProperty("ByteBufPool.registry", "true");
		System.setProperty("ByteBufPool.minSize", "0");
		System.setProperty("ByteBufPool.maxSize", "0");
		System.setProperty("ByteBufPool.clearOnRecycle", "true");
	}

	private static final byte[] BYTES = {'T', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'};

	@Before
	public void clearByteBufPool() {
		ByteBufPool.clear();
	}

	@After
	public void checkByteBufPool() {
		assertEquals(ByteBufPool.getStats().getPoolItemsString(), ByteBufPool.getStats().getCreatedItems(), ByteBufPool.getStats().getPoolItems());
	}

	@Test
	public void testSlice() {
		byte[] bytes = ByteBufStrings.encodeAscii("Hello, World");
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		ByteBuf slice = buf.slice(7, 5);

		assertNotSame(buf, slice);
		assertEquals("World", slice.toString());

		buf = createEmptyByteBufOfSize(16);
		buf.put(bytes);

		slice = buf.slice();

		assertNotSame(buf, slice);
		assertEquals("Hello, World", slice.toString());
	}

	@Test
	public void testEditing() {
		ByteBuf buf = createEmptyByteBufOfSize(256);
		assertEquals(0, buf.head());

		buf.put((byte) 'H');
		assertEquals(1, buf.tail());
		assertEquals('H', buf.at(0));

		buf.put(new byte[]{'e', 'l', 'l', 'o'});
		buf.put(new byte[]{';', ' ', ',', ' ', '.', ' ', '!', ' '}, 2, 2);
		assertEquals(7, buf.tail());

		ByteBuf worldBuf = ByteBuf.wrapForReading(new byte[]{'W', 'o', 'r', 'l', 'd', '!'});
		buf.put(worldBuf);

		assertEquals(worldBuf.limit(), worldBuf.head());
		assertFalse(worldBuf.canWrite());
		assertEquals(13, buf.tail());

		ByteBuf slice = buf.slice();
		ByteBuf newBuf = createEmptyByteBufOfSize(slice.limit());
		slice.drainTo(newBuf, 10);
		assertEquals(10, slice.head());
		assertEquals(10, newBuf.tail());

		slice.drainTo(newBuf, 3);

		assertEquals("Hello, World!", newBuf.toString());
	}

	@Test
	public void transformsToByteBufferInReadMode() {
		ByteBuf buf = createEmptyByteBufOfSize(8);
		buf.tail(5);
		buf.head(2);

		ByteBuffer buffer = buf.toReadByteBuffer();

		assertEquals(2, buffer.position());
		assertEquals(5, buffer.limit());
	}

	@Test
	public void transformsToByteBufferInWriteMode() {
		ByteBuf buf = createEmptyByteBufOfSize(8);
		buf.tail(5);
		buf.head(2);

		ByteBuffer buffer = buf.toWriteByteBuffer();

		assertEquals(5, buffer.position());
		assertEquals(8, buffer.limit());
	}

	@Test
	public void testPoolAndRecycleMechanism() {
		int size = 500;
		ByteBuf buf = ByteBufPool.allocate(size);
		assertNotEquals(size, buf.limit()); // {expected to create 2^N sized bufs only, 500 not in {a}|a == 2^N } => size != limit
		assertEquals(512, buf.limit());

		buf.put(BYTES);

		buf.recycle();

		try {
			buf.put((byte) 'a');
		} catch (AssertionError e) {
			assertEquals(AssertionError.class, e.getClass());
		}

		ByteBuf buf2 = ByteBufPool.allocate(300);
		assertNotEquals(buf, buf2);

		ByteBufPool.clear();
	}

	@Test
	public void testSliceAndRecycleMechanism() {
		ByteBuf buf = ByteBufPool.allocate(5);
		ByteBuf slice0 = buf.slice(1, 3);
		ByteBuf slice01 = slice0.slice(2, 1);
		ByteBuf slice1 = buf.slice(4, 1);

		assertTrue(buf.canWrite());
		slice1.recycle();
		assertTrue(buf.canWrite());
		slice0.recycle();
		assertTrue(buf.canWrite());
		slice01.recycle();
		buf.recycle();
	}

	@Test
	public void testViews() {
		// emulate engine that receives randomly sized bufs from `net` and sends them to some `consumer`

		class MockConsumer {
			private int i = 0;

			private void consume(ByteBuf buf) {
				assertEquals("Test message " + i++, buf.toString());
				buf.recycle();
			}
		}

		MockConsumer consumer = new MockConsumer();
		for (int i = 0; i < 100; i++) {
			ByteBuf buf = ByteBufPool.allocate(32);
			ByteBuffer buffer = buf.toWriteByteBuffer();
			buffer.put(("Test message " + i).getBytes());
			buffer.flip();
			buf.tail(buffer.limit());
			buf.head(buffer.position());
			consumer.consume(buf.slice());
			buf.recycle();
		}
	}

	@Test
	public void testConcat() {
		ByteBuf buf = ByteBufPool.allocate(64);
		buf.put(BYTES);

		ByteBuf secondBuf = ByteBufPool.allocate(32);
		secondBuf.put(BYTES);

		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());
		buf = ByteBufPool.append(buf, secondBuf.slice());

		assertEquals(new String(BYTES)
			+ new String(BYTES)
			+ new String(BYTES)
			+ new String(BYTES)
			+ new String(BYTES)
			+ new String(BYTES), buf.toString());

		buf.recycle();
		secondBuf.recycle();
	}

	@Test
	public void testSkipAndAdvance() {
		ByteBuf buf = createEmptyByteBufOfSize(5);
		buf.put(new byte[]{'a', 'b', 'c'});
		buf.moveHead(2);
		assertEquals('c', buf.get());
		buf.array[3] = 'd';
		buf.array[4] = 'e';
		assertFalse(buf.canRead());
		buf.moveTail(2);
		assertTrue(buf.canRead());
		byte[] bytes = new byte[2];
		buf.drainTo(bytes, 0, 2);
		assertArrayEquals(new byte[]{'d', 'e'}, bytes);
	}

	@Test
	public void testGet() {
		ByteBuf buf = createEmptyByteBufOfSize(3);
		buf.put(new byte[]{'a', 'b', 'c'});

		assertEquals('a', buf.get());
		assertEquals('b', buf.get());
		assertEquals('c', buf.get());
	}

	@Test
	public void testFind() {
		ByteBuf buf = createEmptyByteBufOfSize(BYTES.length);
		buf.put(BYTES);

		assertEquals(5, buf.find((byte) 'm'));
		assertEquals(-1, buf.find(new byte[]{'T', 'e', 's', 's'}));
		assertEquals(1, buf.find(new byte[]{'T', 'e', 's', 's'}, 1, 2));

		assertEquals(0, buf.find(BYTES));
		assertEquals(1, buf.find("est message".getBytes()));
		assertEquals(1, buf.find("_est message__".getBytes(), 1, 11));
	}

	@Test
	public void testClearPool() {
		ByteBufPool.clear();
		List<String> cleared = ByteBufPool.getStats().getPoolSlabs();
		int size = 10;
		ByteBuf[] bufs = new ByteBuf[size];
		for (int i = 0; i < size; i++) {
			bufs[i] = ByteBufPool.allocate(30_000);
		}
		assertNotEquals(cleared.get(17), ByteBufPool.getStats().getPoolSlabs().get(17));
		for (int i = 0; i < size; i++) {
			bufs[i].recycle();
		}
		ByteBufPool.getStats().clear();
		assertEquals(cleared, ByteBufPool.getStats().getPoolSlabs());
	}

	@Test
	public void testClearOnRecycle() {
		ByteBuf byteBuf = ByteBufPool.allocate(4);
		byteBuf.writeByte((byte) 1);
		byteBuf.writeByte((byte) 2);
		byteBuf.writeByte((byte) 3);
		byteBuf.writeByte((byte) 4);
		byte[] array = byteBuf.array();
		assertArrayEquals(new byte[]{1, 2, 3, 4}, array);
		byteBuf.recycle();
		assertArrayEquals(new byte[]{0, 0, 0, 0}, array);
	}

	@Test
	@Ignore("Takes some time and resources")
	// Should not fail with OutOfMemoryError
	public void testProperMemoryFreeing() {
		int bufSize = (int) (Runtime.getRuntime().maxMemory() / 2000);

		//noinspection MismatchedQueryAndUpdateOfCollection
		List<ByteBuf> keptRefs = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			List<ByteBuf> temp = new ArrayList<>();
			for (int j = 0; j < 1000; j++) {
				temp.add(ByteBufPool.allocate(bufSize));
			}
			temp.forEach(ByteBuf::recycle);
			temp.clear();
			ByteBuf newBuf = ByteBufPool.allocate(bufSize);
			keptRefs.add(newBuf);
			ByteBufPool.clear();
		}
		keptRefs.clear();
	}

	private ByteBuf createEmptyByteBufOfSize(int size) {
		return ByteBuf.wrapForWriting(new byte[size]);
	}
}
