package io.activej.memcache.server;

import io.activej.memcache.protocol.MemcacheRpcMessage.Slice;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RingBufferTest {
	private final byte[] BASE_KEY = new byte[]{0};

	@Test
	public void testGetItem() {
		int amountBuffers = 10;
		int bufferCapacity = 100;
		int part = 5;
		int finalNumberItems = part * amountBuffers;
		int eachItemSize = bufferCapacity / part;

		byte[] item = new byte[eachItemSize];
		RingBuffer buffer = RingBuffer.create(amountBuffers, bufferCapacity);
		fillBufferFully(buffer, item, finalNumberItems);

		assertEquals(finalNumberItems, buffer.getItems());
	}

	@Test
	public void testGetSize() {
		int amountBuffers = 10;
		int bufferCapacity = 200;
		int finalNumberItems = 100;
		RingBuffer buffer = RingBuffer.create(amountBuffers, bufferCapacity);

		int expectedSize = 0;
		for (int i = 1; i < finalNumberItems; i += 10) {
			byte[] keyAndValue = new byte[i];
			buffer.put(keyAndValue, keyAndValue);
			expectedSize += i;
		}

		assertEquals(expectedSize, buffer.getSize());
	}

	@Test
	public void testPutToFillTheWholeBuffer() {
		int amountBuffers = 10;
		int bufferCapacity = 100;

		RingBuffer ringBuffer = RingBuffer.create(amountBuffers, bufferCapacity);
		int startNumberAmount = ringBuffer.getItems();

		byte[] itemWhichOccupyTheFullCapacityOfOneBuffer = new byte[bufferCapacity];
		fillBufferFully(ringBuffer, itemWhichOccupyTheFullCapacityOfOneBuffer, amountBuffers);

		int endNumberAmount = ringBuffer.getItems();
		assertEquals(endNumberAmount - startNumberAmount, amountBuffers);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPutToEmptyRingBuffer() {
		RingBuffer ringBuffer = RingBuffer.create(0, 0);

		byte[] keyAndValue = new byte[100];
		ringBuffer.put(BASE_KEY, keyAndValue);
	}

	@Test
	public void testPutHalfOfBufferCapacityItems() {
		int amountBuffers = 10;
		int bufferCapacity = 100;

		RingBuffer ringBuffer = RingBuffer.create(amountBuffers, bufferCapacity);
		byte[] item = new byte[bufferCapacity / 2];
		for (int i = 0; i < amountBuffers * 2; i++) {
			ringBuffer.put(BASE_KEY, item);
		}

		assertEquals(bufferCapacity * amountBuffers, ringBuffer.getSize());
	}

	@Test
	public void testPutTheSameKeyWithFullFillingTheBufferToTestTheReplacingKey() {
		int amountBuffers = 10;
		int bufferCapacity = 100;
		RingBuffer buffer = RingBuffer.create(amountBuffers, bufferCapacity);

		byte[] item = new byte[bufferCapacity];
		for (int i = 0; i < amountBuffers; i++) {
			buffer.put(BASE_KEY, item);
		}

		int amountFilledBuffers = buffer.getItems();
		assertEquals(amountBuffers, amountFilledBuffers);
	}

	@Test
	public void testGet() {
		int amountBuffers = 10;
		int bufferCapacity = 100;
		RingBuffer ringBuffer = RingBuffer.create(amountBuffers, bufferCapacity);

		byte[] item = new byte[bufferCapacity];
		fillBufferFully(ringBuffer, item, amountBuffers);

		for (int i = 0; i < amountBuffers; i++) {
			byte[] key = new byte[i];

			Slice byteBuf = ringBuffer.get(key);
			byte[] array = byteBuf != null ? byteBuf.array() : null;
			if (array == null) {
				fail();
			}

			assertEquals(array.length, item.length);
		}
	}

	@Test
	public void testGetFullCycles() {
		int amountBuffers = 10;
		int bufferCapacity = 100;
		int expectedNumberCycles = 2;
		RingBuffer buffer = RingBuffer.create(amountBuffers, bufferCapacity);

		byte[] item = new byte[bufferCapacity];
		for (int i = 0; i <= expectedNumberCycles; i++) {
			fillBufferFully(buffer, item, amountBuffers);
		}
		assertEquals(expectedNumberCycles, buffer.getFullCycles());
	}

	private void fillBufferFully(RingBuffer buffer, byte[] item, int finalNumberItems) {
		for (int i = 0; i < finalNumberItems; i++) {
			byte[] newKey = new byte[i];
			buffer.put(newKey, item);
		}
	}
}

