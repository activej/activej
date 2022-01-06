package io.activej.crdt;

import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.first;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static org.junit.Assert.assertEquals;

public final class TestSimpleCrdt {
	private CrdtStorageMap<String, TimestampContainer<Integer>> remoteStorage;
	private CrdtServer<String, TimestampContainer<Integer>> server;
	private CrdtStorage<String, TimestampContainer<Integer>> client;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Before
	public void setup() throws IOException {
		remoteStorage = CrdtStorageMap.create(getCurrentEventloop(), TimestampContainer.createCrdtFunction(Integer::max));
		remoteStorage.put("mx", TimestampContainer.now(2));
		remoteStorage.put("test", TimestampContainer.now(3));
		remoteStorage.put("test", TimestampContainer.now(5));
		remoteStorage.put("only_remote", TimestampContainer.now(35));
		remoteStorage.put("only_remote", TimestampContainer.now(4));

		server = CrdtServer.create(getCurrentEventloop(), remoteStorage, new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)));
		server.withListenPort(0).listen();

		client = CrdtStorageClient.create(getCurrentEventloop(), first(server.getBoundAddresses()), new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)));
	}

	@Test
	public void testUpload() {
		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage = CrdtStorageMap.create(getCurrentEventloop(), TimestampContainer.createCrdtFunction(Integer::max));
		localStorage.put("mx", TimestampContainer.now(22));
		localStorage.put("mx", TimestampContainer.now(2));
		localStorage.put("mx", TimestampContainer.now(23));
		localStorage.put("test", TimestampContainer.now(1));
		localStorage.put("test", TimestampContainer.now(2));
		localStorage.put("test", TimestampContainer.now(4));
		localStorage.put("test", TimestampContainer.now(3));
		localStorage.put("only_local", TimestampContainer.now(47));
		localStorage.put("only_local", TimestampContainer.now(12));

		await(StreamSupplier.ofIterator(localStorage.iterator())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(server::close));

		System.out.println("Data at 'remote' storage:");
		remoteStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(23, checkNotNull(remoteStorage.get("mx")).getState().intValue());
		assertEquals(5, checkNotNull(remoteStorage.get("test")).getState().intValue());
		assertEquals(35, checkNotNull(remoteStorage.get("only_remote")).getState().intValue());
		assertEquals(47, checkNotNull(remoteStorage.get("only_local")).getState().intValue());
	}

	@Test
	public void testDownload() {
		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage = CrdtStorageMap.create(getCurrentEventloop(), TimestampContainer.createCrdtFunction(Integer::max));

		await(client.download().then(supplier -> supplier
				.streamTo(StreamConsumer.ofConsumer(localStorage::put))
				.whenComplete(server::close)));

		System.out.println("Data fetched from 'remote' storage:");
		localStorage.iterator().forEachRemaining(System.out::println);

		assertEquals(2, checkNotNull(localStorage.get("mx")).getState().intValue());
		assertEquals(5, checkNotNull(localStorage.get("test")).getState().intValue());
		assertEquals(35, checkNotNull(localStorage.get("only_remote")).getState().intValue());
	}
}
