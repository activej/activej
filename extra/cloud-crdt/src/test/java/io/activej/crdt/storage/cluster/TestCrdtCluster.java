package io.activej.crdt.storage.cluster;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.net.AbstractReactiveServer;
import io.activej.reactor.nio.NioReactor;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.BinarySerializers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;

import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;

public final class TestCrdtCluster {
	private static final BinarySerializer<Set<Integer>> INT_SET_SERIALIZER = BinarySerializers.ofSet(INT_SERIALIZER);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public static final int REPLICATION_COUNT = 4;
	public static final int CLIENT_SERVER_PAIRS = 10;

	@Test
	public void testUpload() throws IOException {
		NioReactor reactor = getCurrentReactor();

		CrdtDataSerializer<String, Integer> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER);

		List<CrdtServer<String, Integer>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, Integer>> clients = new HashMap<>();
		Map<String, CrdtStorageMap<String, Integer>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			CrdtStorageMap<String, Integer> storage = CrdtStorageMap.create(reactor, ignoringTimestamp(Math::max));
			InetSocketAddress address = new InetSocketAddress(getFreePort());
			CrdtServer<String, Integer> server = CrdtServer.create(reactor, storage, serializer)
					.withListenAddresses(address);
			server.listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(reactor, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}
		clients.put("dead_one", CrdtStorageClient.create(reactor, new InetSocketAddress(5555), serializer).withConnectTimeout(Duration.ofSeconds(1)));
		clients.put("dead_two", CrdtStorageClient.create(reactor, new InetSocketAddress(5556), serializer).withConnectTimeout(Duration.ofSeconds(1)));
		clients.put("dead_three", CrdtStorageClient.create(reactor, new InetSocketAddress(5557), serializer).withConnectTimeout(Duration.ofSeconds(1)));

		List<CrdtData<String, Integer>> data = new ArrayList<>();
		long now = reactor.currentTimeMillis();
		for (int i = 0; i < 25; i++) {
			data.add(new CrdtData<>((char) (i + 97) + "", now, i + 1));
		}
		CrdtStorageMap<String, Integer> localStorage = CrdtStorageMap.create(reactor, ignoringTimestamp(Math::max));
		for (CrdtData<String, Integer> datum : data) {
			localStorage.put(datum);
		}
		CrdtStorageCluster<String, Integer, String> cluster = CrdtStorageCluster.create(
				reactor,
				DiscoveryService.of(
						RendezvousPartitionScheme.<String>create()
								.withPartitionGroup(
										RendezvousPartitionGroup.create(clients.keySet())
												.withReplicas(REPLICATION_COUNT)
												.withRepartition(true))
								.withCrdtProvider(clients::get)
				),
				ignoringTimestamp(Integer::max));

		await(cluster.start()
				.then(() -> StreamSupplier.ofIterator(localStorage.iterator())
						.streamTo(StreamConsumer.ofPromise(cluster.upload())))
				.whenComplete(() -> servers.forEach(AbstractReactiveServer::close)));

		Map<CrdtData<String, Integer>, Integer> result = new HashMap<>();

		remoteStorages.values().forEach(v -> v.iterator()
				.forEachRemaining(x -> result.compute(x, ($, count) -> count == null ? 1 : (count + 1))));

		assertEquals(Set.copyOf(data), result.keySet());
		for (Integer count : result.values()) {
			assertEquals(REPLICATION_COUNT, count.intValue());
		}
	}

	@Test
	public void testDownload() throws IOException {
		NioReactor reactor = getCurrentReactor();

		List<CrdtServer<String, Set<Integer>>> servers = new ArrayList<>();
		Map<String, CrdtStorage<String, Set<Integer>>> clients = new HashMap<>();

		CrdtFunction<Set<Integer>> union = ignoringTimestamp((a, b) -> {
			a.addAll(b);
			return a;
		});
		CrdtDataSerializer<String, Set<Integer>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SET_SERIALIZER);

		String key1 = "test_1";
		String key2 = "test_2";
		String key3 = "test_3";

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			CrdtStorageMap<String, Set<Integer>> storage = CrdtStorageMap.create(reactor, union);

			storage.put(key1, Set.of(i));
			storage.put(key2, Set.of(i / 2));
			storage.put(key3, Set.of(123));

			InetSocketAddress address = new InetSocketAddress(getFreePort());
			CrdtServer<String, Set<Integer>> server = CrdtServer.create(reactor, storage, serializer);
			server.withListenAddresses(address).listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorageClient.create(reactor, address, serializer).withConnectTimeout(Duration.ofSeconds(1)));
		}

		clients.put("dead_one", CrdtStorageClient.create(reactor, new InetSocketAddress(5555), serializer).withConnectTimeout(Duration.ofSeconds(1)));
		clients.put("dead_two", CrdtStorageClient.create(reactor, new InetSocketAddress(5556), serializer).withConnectTimeout(Duration.ofSeconds(1)));
		clients.put("dead_three", CrdtStorageClient.create(reactor, new InetSocketAddress(5557), serializer).withConnectTimeout(Duration.ofSeconds(1)));

		CrdtStorageMap<String, Set<Integer>> localStorage = CrdtStorageMap.create(reactor, union);
		CrdtStorageCluster<String, Set<Integer>, String> cluster = CrdtStorageCluster.create(reactor,
				DiscoveryService.of(
						RendezvousPartitionScheme.<String>create()
								.withPartitionGroup(RendezvousPartitionGroup.create(clients.keySet()).withReplicas(REPLICATION_COUNT).withRepartition(true))
								.withCrdtProvider(clients::get)),
				union);

		await(cluster.start()
				.then(() -> cluster.download())
				.then(supplier -> supplier
						.streamTo(StreamConsumer.ofConsumer(localStorage::put)))
				.whenComplete(() -> servers.forEach(AbstractReactiveServer::close)));

		assertEquals(Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), localStorage.get(key1));
		assertEquals(Set.of(0, 1, 2, 3, 4), localStorage.get(key2));
		assertEquals(Set.of(123), localStorage.get(key3));
	}
}
