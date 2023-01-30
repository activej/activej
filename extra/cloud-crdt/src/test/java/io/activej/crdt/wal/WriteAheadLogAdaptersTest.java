package io.activej.crdt.wal;

import io.activej.promise.Promise;
import io.activej.reactor.ImplicitlyReactive;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class WriteAheadLogAdaptersTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testFlushOnUpdatesCount() {
		StubWriteAheadLog walStub = new StubWriteAheadLog();
		int updatesCount = 100;
		IWriteAheadLog<Integer, Integer> wal = WriteAheadLogAdapters.flushOnUpdatesCount(walStub, updatesCount);

		for (int i = 0; i < 100; i++) {
			int j = 0;
			for (; j < updatesCount - 1; j++) {
				await(wal.put(j, j));
				assertNotEquals(0, walStub.updatesCount);
			}
			await(wal.put(j, j));
			assertEquals(0, walStub.updatesCount);
		}
	}

	private static final class StubWriteAheadLog extends ImplicitlyReactive
			implements IWriteAheadLog<Integer, Integer> {
		private int updatesCount;

		@Override
		public Promise<Void> put(Integer key, Integer value) {
			checkInReactorThread(this);
			updatesCount++;
			return Promise.complete();
		}

		@Override
		public Promise<Void> flush() {
			checkInReactorThread(this);
			updatesCount = 0;
			return Promise.complete();
		}
	}
}
