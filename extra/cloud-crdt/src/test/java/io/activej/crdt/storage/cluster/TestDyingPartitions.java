package io.activej.crdt.storage.cluster;

import io.activej.async.process.AsyncCloseable;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.time.Duration;
import java.util.*;

import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class TestDyingPartitions {
	private static final int SERVER_COUNT = 5;
	private static final int REPLICATION_COUNT = 3;
	private static final CrdtFunction<TimestampContainer<Integer>> CRDT_FUNCTION = TimestampContainer.createCrdtFunction(Integer::max);
	private static final CrdtDataSerializer<String, TimestampContainer<Integer>> SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Map<Integer, AbstractServer<?>> servers;
	private Map<Integer, CrdtStorageMap<String, TimestampContainer<Integer>>> storages;
	private CrdtStorageCluster<String, TimestampContainer<Integer>> cluster;

	@Before
	public void setUp() throws Exception {
		servers = new LinkedHashMap<>();
		storages = new LinkedHashMap<>();

		Map<String, CrdtStorage<String, TimestampContainer<Integer>>> clients = new HashMap<>();

		for (int i = 0; i < SERVER_COUNT; i++) {
			int port = getFreePort();
			Eventloop eventloop = Eventloop.create();
			CrdtStorageMap<String, TimestampContainer<Integer>> storage = CrdtStorageMap.create(eventloop, CRDT_FUNCTION);
			InetSocketAddress address = new InetSocketAddress(port);
			CrdtServer<String, TimestampContainer<Integer>> server = CrdtServer.create(eventloop, storage, SERIALIZER);
			server.withListenAddresses(address).listen();
			assertNull(servers.put(port, server));
			assertNull(storages.put(port, storage));
			new Thread(eventloop).start();

			clients.put("server_" + i, CrdtStorageClient.create(eventloop, address, SERIALIZER));
		}

		CrdtPartitions<String, TimestampContainer<Integer>> partitions = CrdtPartitions.create(Eventloop.getCurrentEventloop(), clients);
		cluster = CrdtStorageCluster.create(partitions, CRDT_FUNCTION)
				.withReplicationCount(REPLICATION_COUNT);

	}

	@Test
	public void testUploadWithDyingPartitions() {
		List<CrdtData<String, TimestampContainer<Integer>>> data = new ArrayList<>();
		for (int i = 0; i < 100_000; i++) {
			data.add(new CrdtData<>(String.valueOf(i), TimestampContainer.now(i + 1)));
		}

		Promise<Void> uploadPromise = StreamSupplier.ofIterator(data.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload()));

		Eventloop.getCurrentEventloop().delay(Duration.ofMillis(50), this::shutdown2Servers);

		await(uploadPromise);
		assertEquals(2, cluster.getPartitions().getDeadPartitions().size());

		Set<CrdtData<String, TimestampContainer<Integer>>> result = new HashSet<>();
		for (CrdtStorageMap<String, TimestampContainer<Integer>> storage : storages.values()) {
			storage.iterator().forEachRemaining(result::add);
		}

		assertEquals(new HashSet<>(data), result);
		shutdownAllEventloops();
	}

	@Test
	public void testDownloadWithDyingPartitions() {
		List<CrdtData<String, TimestampContainer<Integer>>> data = new ArrayList<>();
		for (int i = 0; i < 500_000; i++) {
			data.add(new CrdtData<>(String.valueOf(i), TimestampContainer.now(i + 1)));
		}

		await(StreamSupplier.ofIterator(data.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload())));

		Promise<List<CrdtData<String, TimestampContainer<Integer>>>> downloadPromise = cluster.download()
				.then(StreamSupplier::toList);

		Eventloop.getCurrentEventloop().delay(Duration.ofMillis(10), this::shutdown2Servers);

		List<CrdtData<String, TimestampContainer<Integer>>> downloaded = await(downloadPromise);

		assertEquals(new HashSet<>(data), new HashSet<>(downloaded));
		shutdownAllEventloops();
	}

	@SuppressWarnings("ConstantConditions")
	private void shutdown2Servers() {
		Iterator<AbstractServer<?>> serverIterator = servers.values().iterator();
		for (int i = 0; i < 2; i++) {
			AbstractServer<?> server = serverIterator.next();
			Eventloop eventloop = server.getEventloop();
			eventloop.execute(() -> {
				for (SelectionKey key : eventloop.getSelector().keys()) {
					Object attachment = key.attachment();
					if (attachment instanceof AsyncCloseable){
						((AsyncCloseable) attachment).close();
					}
				}
			});
		}
	}

	private void shutdownAllEventloops() {
		for (AbstractServer<?> server : servers.values()) {
			Eventloop eventloop = server.getEventloop();
			eventloop.execute(() -> {
				server.close();
				eventloop.breakEventloop();
			});
			try {
				Thread eventloopThread = eventloop.getEventloopThread();
				if (eventloopThread != null) {
					eventloopThread.join();
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
