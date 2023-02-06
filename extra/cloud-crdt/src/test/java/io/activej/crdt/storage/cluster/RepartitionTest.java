package io.activej.crdt.storage.cluster;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtEntity;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.local.MapCrdtStorage;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

public final class RepartitionTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		Reactor reactor = getCurrentReactor();
		CrdtFunction<Long> crdtFunction = ignoringTimestamp(Long::max);

		Map<String, MapCrdtStorage<String, Long>> clients = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			MapCrdtStorage<String, Long> client = MapCrdtStorage.create(reactor, crdtFunction);
			clients.put("client_" + i, client);
		}
		String localPartitionId = "client_0";
		long now = reactor.currentTimeMillis();
		List<CrdtData<String, Long>> data = LongStream.range(1, 100)
				.mapToObj(i -> new CrdtData<>("test" + i, now, i)).toList();
		await(StreamSuppliers.ofIterator(data.iterator())
				.streamTo(StreamConsumers.ofPromise(clients.get(localPartitionId).upload())));

		int replicationCount = 3;
		ClusterCrdtStorage<String, Long, String> cluster = ClusterCrdtStorage.create(reactor,
				IDiscoveryService.of(
						RendezvousPartitionScheme.<String>builder()
								.withPartitionGroup(RendezvousPartitionGroup.builder(clients.keySet())
										.withReplicas(replicationCount)
										.build())
								.withCrdtProvider(clients::get)
								.build()),
				crdtFunction);
		await(cluster.start());

		await(CrdtRepartitionController.create(reactor, cluster, localPartitionId).repartition());

		Map<String, Integer> counts = new HashMap<>();
		Map<String, Long> states = new HashMap<>();

		clients.values().forEach(v -> v.iterator()
				.forEachRemaining(x -> {
					counts.compute(x.getKey(), ($, count) -> count == null ? 1 : (count + 1));
					states.compute(x.getKey(), ($, state) -> {
						if (state != null) {
							assertEquals(state, x.getState());
						}
						return x.getState();
					});
				}));

		Map<String, Long> expectedStates = data.stream().collect(toMap(CrdtEntity::getKey, CrdtData::getState));
		assertEquals(expectedStates, states);

		for (Integer count : counts.values()) {
			assertEquals(replicationCount, count.intValue());
		}
	}
}
