package io.activej.crdt.storage.cluster;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtEntity;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

public final class RepartitionTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		CrdtFunction<Long> crdtFunction = ignoringTimestamp(Long::max);

		Map<String, CrdtStorageMap<String, Long>> clients = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, Long> client = CrdtStorageMap.create(Eventloop.getCurrentEventloop(), crdtFunction);
			clients.put("client_" + i, client);
		}
		String localPartitionId = "client_0";
		long now = Eventloop.getCurrentEventloop().currentTimeMillis();
		List<CrdtData<String, Long>> data = LongStream.range(1, 100)
				.mapToObj(i -> new CrdtData<>("test" + i, now, i))
				.collect(Collectors.toList());
		await(StreamSupplier.ofIterator(data.iterator())
				.streamTo(StreamConsumer.ofPromise(clients.get(localPartitionId).upload())));

		int replicationCount = 3;
		CrdtStorageCluster<String, Long, String> cluster = CrdtStorageCluster.create(Eventloop.getCurrentEventloop(),
				DiscoveryService.of(
						RendezvousPartitionScheme.<String>create()
								.withPartitionGroup(RendezvousPartitionGroup.create(clients.keySet()).withReplicas(replicationCount))
								.withCrdtProvider(clients::get)),
				crdtFunction);
		await(cluster.start());

		await(CrdtRepartitionController.create(cluster, localPartitionId).repartition());

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
