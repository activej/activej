package io.activej.etcd;

import io.activej.common.ref.Ref;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class WatchTransactionStressTest {

	public static final int WRITE_THREADS = 10;
	// https://github.com/etcd-io/etcd/blob/149bcb75ea6d0e62c6c393a05a6b0c64efeda1db/server/etcdmain/help.go#L84
	public static final int TXN_LENGTH = 128;

	public static final List<String> keys = IntStream.range(0, TXN_LENGTH)
		.mapToObj(i -> "test." + WatchTransactionStressTest.class.getName() + "." + i)
		.toList();

	@Test
	public void testRevisionOrderInTransaction_10() throws InterruptedException {
		doTest(10);
	}

	@Test
	public void testRevisionOrderInTransaction_100() throws InterruptedException {
		doTest(100);
	}

	@Test
	@Ignore("Takes too long")
	public void testRevisionOrderInTransaction_1_000() throws InterruptedException {
		doTest(1000);
	}

	@Test
	@Ignore("Takes too long")
	public void testRevisionOrderInTransaction10_000() throws InterruptedException {
		doTest(10_000);
	}

	private static void doTest(int txnCount) throws InterruptedException {
		List<Client> clients = IntStream.range(0, WRITE_THREADS)
			.mapToObj($ -> createClient())
			.toList();

		List<Thread> threads = new ArrayList<>(WRITE_THREADS);
		for (int i = 0; i < WRITE_THREADS; i++) {
			Client client = clients.get(i);
			threads.add(new Thread(() -> {
				for (int j = 0; j < txnCount; j++) {
					Op[] ops = new Op[TXN_LENGTH];
					for (int k = 0; k < TXN_LENGTH; k++) {
						ByteSequence key = ByteSequence.from(keys.get(k), UTF_8);
						ByteSequence value = ByteSequence.from(UUID.randomUUID().toString(), UTF_8);
						ops[k] = Op.put(key, value, PutOption.DEFAULT);
					}

					try {
						client.getKVClient()
							.txn()
							.Then(ops)
							.commit().get();
					} catch (InterruptedException | ExecutionException e) {
						throw new RuntimeException(e);
					}
				}
			}));
		}

		Ref<Throwable> errorRef = new Ref<>();
		Client client = createClient();
		Watch.Watcher watch = client.getWatchClient().watch(
			ByteSequence.from("test.", UTF_8),
			WatchOption.builder()
				.isPrefix(true)
				.build(),
			new Watch.Listener() {
				private long expectedRevision = -1;

				@Override
				public void onNext(WatchResponse response) {
					try {
						long currentRevision = -1;
						for (WatchEvent event : response.getEvents()) {
							long modRevision = event.getKeyValue().getModRevision();
							if (currentRevision != -1) {
								assertTrue(modRevision == currentRevision || modRevision == (currentRevision + 1));
							}
							if (expectedRevision != -1) {
								assertEquals(modRevision, expectedRevision + 1);
							}
							currentRevision = modRevision;
						}
						expectedRevision = currentRevision;
					} catch (Throwable e) {
						errorRef.set(e);
						throw e;
					}
				}

				@Override
				public void onError(Throwable throwable) {
					errorRef.set(throwable);
				}

				@Override
				public void onCompleted() {
				}
			});

		for (Thread thread : threads) {
			thread.start();
		}

		for (Thread thread : threads) {
			thread.join();
		}

		assertNull(errorRef.get());
		watch.close();
	}

	private static Client createClient() {
		return Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();
	}
}
