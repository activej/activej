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
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.channels.SelectionKey;
import java.util.*;

import static io.activej.common.Utils.first;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static org.junit.Assert.assertEquals;

public final class TestDyingPartitions {
	private static final int SERVER_COUNT = 5;
	private static final int REPLICATION_COUNT = 3;
	private static final CrdtFunction<TimestampContainer<Integer>> CRDT_FUNCTION = TimestampContainer.createCrdtFunction(Integer::max);
	private static final CrdtDataSerializer<String, TimestampContainer<Integer>> SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private List<AbstractServer<?>> servers;
	private List<CrdtStorageMap<String, TimestampContainer<Integer>>> storages;
	private CrdtStorageCluster<String, TimestampContainer<Integer>, String> cluster;

	@Before
	public void setUp() throws Exception {
		servers = new ArrayList<>();
		storages = new ArrayList<>();

		Map<String, CrdtStorage<String, TimestampContainer<Integer>>> clients = new HashMap<>();

		for (int i = 0; i < SERVER_COUNT; i++) {
			Eventloop eventloop = Eventloop.create();
			CrdtStorageMap<String, TimestampContainer<Integer>> storage = CrdtStorageMap.create(eventloop, CRDT_FUNCTION);
			CrdtServer<String, TimestampContainer<Integer>> server = CrdtServer.create(eventloop, storage, SERIALIZER);
			server.withListenPort(0).listen();
			servers.add(server);
			storages.add(storage);
			new Thread(eventloop).start();

			clients.put("server_" + i, CrdtStorageClient.create(eventloop, first(server.getBoundAddresses()), SERIALIZER));
		}

		DiscoveryService<String, TimestampContainer<Integer>, String> discoveryService = DiscoveryService.constant(clients);
		CrdtPartitions<String, TimestampContainer<Integer>, String> partitions = CrdtPartitions.create(Eventloop.getCurrentEventloop(), discoveryService);
		await(partitions.start());
		cluster = CrdtStorageCluster.create(partitions, CRDT_FUNCTION)
				.withReplicationCount(REPLICATION_COUNT);
	}

	@Test
	public void testUploadWithDyingPartitions() {
		List<CrdtData<String, TimestampContainer<Integer>>> data = new ArrayList<>();
		for (int i = 0; i < 100_000; i++) {
			data.add(new CrdtData<>(String.valueOf(i), TimestampContainer.now(i + 1)));
		}

		await(StreamSupplier.ofIterator(data.iterator())
				.streamTo(StreamConsumer.ofPromise(cluster.upload()
						.whenResult(this::shutdown2Servers))));

		assertEquals(2, cluster.getPartitions().getDeadPartitions().size());

		Set<CrdtData<String, TimestampContainer<Integer>>> result = new HashSet<>();
		for (CrdtStorageMap<String, TimestampContainer<Integer>> storage : storages) {
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

		List<CrdtData<String, TimestampContainer<Integer>>> downloaded = await(cluster.download()
				.whenResult(this::shutdown2Servers)
				.then(StreamSupplier::toList));

		assertEquals(new HashSet<>(data), new HashSet<>(downloaded));
		shutdownAllEventloops();
	}

	@SuppressWarnings("ConstantConditions")
	private void shutdown2Servers() {
		Iterator<AbstractServer<?>> serverIterator = servers.iterator();
		for (int i = 0; i < 2; i++) {
			AbstractServer<?> server = serverIterator.next();
			Eventloop eventloop = server.getEventloop();
			eventloop.execute(() -> {
				for (SelectionKey key : eventloop.getSelector().keys()) {
					Object attachment = key.attachment();
					if (attachment instanceof AsyncCloseable) {
						((AsyncCloseable) attachment).close();
					}
				}
			});
		}
	}

	private void shutdownAllEventloops() {
		for (AbstractServer<?> server : servers) {
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
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}
	}
}
