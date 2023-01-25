package io.activej.crdt;

import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.local.CrdtStorage_Map;
import io.activej.crdt.util.BinarySerializer_CrdtData;
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
	private CrdtStorage_Map<String, Integer> remoteStorage;
	private CrdtServer<String, Integer> server;
	private ICrdtStorage<String, Integer> client;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setup() throws IOException {
		remoteStorage = CrdtStorage_Map.create(getCurrentReactor(), ignoringTimestamp(Integer::max));
		remoteStorage.put("mx", 2);
		remoteStorage.put("test", 3);
		remoteStorage.put("test", 5);
		remoteStorage.put("only_remote", 35);
		remoteStorage.put("only_remote", 4);

		BinarySerializer_CrdtData<String, Integer> serializer = new BinarySerializer_CrdtData<>(UTF8_SERIALIZER, INT_SERIALIZER);
		int port = getFreePort();
		server = CrdtServer.builder(getCurrentReactor(), remoteStorage, serializer)
				.withListenAddress(new InetSocketAddress(port))
				.build();
		server.listen();

		client = CrdtStorage_Client.create(getCurrentReactor(), new InetSocketAddress(port), serializer);
	}

	@Test
	public void testPing() {
		await(client.ping().whenComplete(server::close));
	}

	@Test
	public void testUpload() {
		CrdtStorage_Map<String, Integer> localStorage = CrdtStorage_Map.create(getCurrentReactor(), ignoringTimestamp(Integer::max));
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
		CrdtStorage_Map<String, Integer> localStorage = CrdtStorage_Map.create(getCurrentReactor(), ignoringTimestamp(Integer::max));

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
		CrdtStorage_Map<String, Integer> localStorage = CrdtStorage_Map.create(getCurrentReactor(), ignoringTimestamp(Integer::max));

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
