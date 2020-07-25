package io.activej.eventloop.util;

import org.junit.Before;
import org.junit.Test;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;

import static io.activej.common.Checks.checkNotNull;
import static org.junit.Assert.*;

public class OptimizedSelectedKeysSetTest {
	private OptimizedSelectedKeysSet set;

	@Before
	public void init() {
		set = new OptimizedSelectedKeysSet();
	}

	@Test
	public void testAdd() {
		int size = 100;
		fillSet(size);

		for (int i = 0; i < size; i++) {
			SimpleSelectionKey key = (SimpleSelectionKey) set.get(i);

			assertEquals(i, checkNotNull(key).id);
		}
	}

	@Test
	public void testClear() {
		int size = 100;
		fillSet(size);

		set.clear();
		assertEquals(0, set.size());
	}

	@Test
	public void testIterator() {
		int size = 100;
		Iterator<SelectionKey> iterator = set.iterator();
		fillSet(size);

		while (iterator.hasNext()) {
			SelectionKey next = iterator.next();
			assertNotNull(next);
		}

		set.clear();
		iterator = set.iterator();
		assertFalse(iterator.hasNext());
	}

	private void fillSet(int size) {
		for (int i = 0; i < size; i++) {
			set.add(new SimpleSelectionKey(i));
		}
	}

	private static class SimpleSelectionKey extends SelectionKey {
		int id;

		SimpleSelectionKey(int id) {
			this.id = id;
		}

		public SelectableChannel channel() {
			return null;
		}

		public Selector selector() {
			return null;
		}

		public boolean isValid() { return false; }

		public void cancel() { }

		public int interestOps() {
			return 0;
		}

		public SelectionKey interestOps(int ops) {
			return null;
		}

		public int readyOps() {
			return 0;
		}
	}
}


