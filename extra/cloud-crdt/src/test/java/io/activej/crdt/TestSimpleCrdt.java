package io.activej.crdt;

import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class TestSimpleCrdt {
	private CrdtStorageMap<String, Integer> remoteStorage;
	private CrdtServer<String, Integer> server;
	private AsyncCrdtStorage<String, Integer> client;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setup() throws IOException {
		remoteStorage = CrdtStorageMap.create(getCurrentReactor(), ignoringTimestamp(Integer::max));
		remoteStorage.put("mx", 2);
		remoteStorage.put("test", 3);
		remoteStorage.put("test", 5);
		remoteStorage.put("only_remote", 35);
		remoteStorage.put("only_remote", 4);

		CrdtDataSerializer<String, Integer> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER);
		server = CrdtServer.create(getCurrentReactor(), remoteStorage, serializer);
		int port = getFreePort();
		server.withListenAddress(new InetSocketAddress(port)).listen();

		client = CrdtStorageClient.create(getCurrentReactor(), new InetSocketAddress(port), serializer);
	}

	@Test
	public void testPing() {
		await(client.ping().whenComplete(server::close));
	}

	@Test
	public void testUpload() {
		CrdtStorageMap<String, Integer> localStorage = CrdtStorageMap.create(getCurrentReactor(), ignoringTimestamp(Integer::max));
		localStorage.put("mx", 22);
		localStorage.put("mx", 2);
		localStorage.put("mx", 23);
		localStorage.put("test", 1);
		localStorage.put("test", 2);
		localStorage.put("test", 4);
		localStorage.put("test", 3);
		localStorage.put("only_local", 47);
		localStorage.put("only_local", 12);

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(server::close));

		System.out.println("Data at 'remote' storage:");
		remoteStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(23, checkNotNull(remoteStorage.get("mx")).intValue());
		assertEquals(5, checkNotNull(remoteStorage.get("test")).intValue());
		assertEquals(35, checkNotNull(remoteStorage.get("only_remote")).intValue());
		assertEquals(47, checkNotNull(remoteStorage.get("only_local")).intValue());
	}

	@Test
	public void testDownload() {
		CrdtStorageMap<String, Integer> localStorage = CrdtStorageMap.create(getCurrentReactor(), ignoringTimestamp(Integer::max));

		await(client.download().then(supplier -> supplier
				.streamTo(StreamConsumer.ofConsumer(localStorage::put))
				.whenComplete(server::close)));

		System.out.println("Data fetched from 'remote' storage:");
		localStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(2, checkNotNull(localStorage.get("mx")).intValue());
		assertEquals(5, checkNotNull(localStorage.get("test")).intValue());
		assertEquals(35, checkNotNull(localStorage.get("only_remote")).intValue());
	}

	@Test
	public void testTake() {
		CrdtStorageMap<String, Integer> localStorage = CrdtStorageMap.create(getCurrentReactor(), ignoringTimestamp(Integer::max));

		await(client.take().then(supplier -> supplier
				.streamTo(StreamConsumer.ofConsumer(localStorage::put))
				.then(() -> client.download().then(StreamSupplier::toList)
						.whenResult(afterTake -> assertTrue(afterTake.isEmpty())))
				.whenComplete(server::close)));

		assertEquals(2, checkNotNull(localStorage.get("mx")).intValue());
		assertEquals(5, checkNotNull(localStorage.get("test")).intValue());
		assertEquals(35, checkNotNull(localStorage.get("only_remote")).intValue());

	}
}
