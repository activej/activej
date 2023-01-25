package io.activej.crdt.storage.cluster;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorage_Client;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.local.CrdtStorage_Map;
import io.activej.crdt.util.BinarySerializer_CrdtData;
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

		BinarySerializer_CrdtData<String, Integer> serializer = new BinarySerializer_CrdtData<>(UTF8_SERIALIZER, INT_SERIALIZER);

		List<CrdtServer<String, Integer>> servers = new ArrayList<>();
		Map<String, ICrdtStorage<String, Integer>> clients = new HashMap<>();
		Map<String, CrdtStorage_Map<String, Integer>> remoteStorages = new LinkedHashMap<>();
		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			CrdtStorage_Map<String, Integer> storage = CrdtStorage_Map.create(reactor, ignoringTimestamp(Math::max));
			InetSocketAddress address = new InetSocketAddress(getFreePort());
			CrdtServer<String, Integer> server = CrdtServer.builder(reactor, storage, serializer)
					.withListenAddresses(address)
					.build();
			server.listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorage_Client.create(reactor, address, serializer));
			remoteStorages.put("server_" + i, storage);
		}
		clients.put("dead_one", CrdtStorage_Client.builder(reactor, new InetSocketAddress(5555), serializer)
				.withConnectTimeout(Duration.ofSeconds(1))
				.build());
		clients.put("dead_two", CrdtStorage_Client.builder(reactor, new InetSocketAddress(5556), serializer)
				.withConnectTimeout(Duration.ofSeconds(1))
				.build());
		clients.put("dead_three", CrdtStorage_Client.builder(reactor, new InetSocketAddress(5557), serializer)
				.withConnectTimeout(Duration.ofSeconds(1))
				.build());

		List<CrdtData<String, Integer>> data = new ArrayList<>();
		long now = reactor.currentTimeMillis();
		for (int i = 0; i < 25; i++) {
			data.add(new CrdtData<>((char) (i + 97) + "", now, i + 1));
		}
		CrdtStorage_Map<String, Integer> localStorage = CrdtStorage_Map.create(reactor, ignoringTimestamp(Math::max));
		for (CrdtData<String, Integer> datum : data) {
			localStorage.put(datum);
		}
		CrdtStorage_Cluster<String, Integer, String> cluster = CrdtStorage_Cluster.create(
				reactor,
				IDiscoveryService.of(
						PartitionScheme_Rendezvous.<String>builder()
								.withPartitionGroup(
										RendezvousPartitionGroup.builder(clients.keySet())
												.withReplicas(REPLICATION_COUNT)
												.withRepartition(true)
												.build())
								.withCrdtProvider(clients::get)
								.build()
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
		Map<String, ICrdtStorage<String, Set<Integer>>> clients = new HashMap<>();

		CrdtFunction<Set<Integer>> union = ignoringTimestamp((a, b) -> {
			a.addAll(b);
			return a;
		});
		BinarySerializer_CrdtData<String, Set<Integer>> serializer = new BinarySerializer_CrdtData<>(UTF8_SERIALIZER, INT_SET_SERIALIZER);

		String key1 = "test_1";
		String key2 = "test_2";
		String key3 = "test_3";

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			CrdtStorage_Map<String, Set<Integer>> storage = CrdtStorage_Map.create(reactor, union);

			storage.put(key1, Set.of(i));
			storage.put(key2, Set.of(i / 2));
			storage.put(key3, Set.of(123));

			InetSocketAddress address = new InetSocketAddress(getFreePort());
			CrdtServer<String, Set<Integer>> server = CrdtServer.builder(reactor, storage, serializer)
					.withListenAddresses(address)
					.build();
			server.listen();
			servers.add(server);
			clients.put("server_" + i, CrdtStorage_Client.builder(reactor, address, serializer)
					.withConnectTimeout(Duration.ofSeconds(1))
					.build());
		}

		clients.put("dead_one", CrdtStorage_Client.builder(reactor, new InetSocketAddress(5555), serializer)
				.withConnectTimeout(Duration.ofSeconds(1))
				.build());
		clients.put("dead_two", CrdtStorage_Client.builder(reactor, new InetSocketAddress(5556), serializer)
				.withConnectTimeout(Duration.ofSeconds(1))
				.build());
		clients.put("dead_three", CrdtStorage_Client.builder(reactor, new InetSocketAddress(5557), serializer)
				.withConnectTimeout(Duration.ofSeconds(1))
				.build());

		CrdtStorage_Map<String, Set<Integer>> localStorage = CrdtStorage_Map.create(reactor, union);
		CrdtStorage_Cluster<String, Set<Integer>, String> cluster = CrdtStorage_Cluster.create(reactor,
				IDiscoveryService.of(
						PartitionScheme_Rendezvous.<String>builder()
								.withPartitionGroup(RendezvousPartitionGroup.builder(clients.keySet())
										.withReplicas(REPLICATION_COUNT)
										.withRepartition(true)
										.build())
								.withCrdtProvider(clients::get)
								.build()),
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
