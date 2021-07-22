package io.activej.crdt.storage.cluster;

import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public final class RepartitionTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		CrdtFunction<TimestampContainer<Integer>> crdtFunction = TimestampContainer.createCrdtFunction(Integer::max);

		Map<String, CrdtStorageMap<String, TimestampContainer<Integer>>> clients = new LinkedHashMap<>();
		for (int i = 0; i < 10; i++) {
			CrdtStorageMap<String, TimestampContainer<Integer>> client = CrdtStorageMap.create(Eventloop.getCurrentEventloop(), crdtFunction);
			clients.put("client_" + i, client);
		}
		String localPartitionId = "client_0";
		List<CrdtData<String, TimestampContainer<Integer>>> data = IntStream.range(1, 100)
				.mapToObj(i -> new CrdtData<>("test" + i, TimestampContainer.now(i)))
				.collect(Collectors.toList());
		await(StreamSupplier.ofIterator(data.iterator())
				.streamTo(StreamConsumer.ofPromise(clients.get(localPartitionId).upload())));

		CrdtPartitions<String, TimestampContainer<Integer>> partitions = CrdtPartitions.create(Eventloop.getCurrentEventloop(), clients);
		int replicationCount = 3;
		CrdtStorageCluster<String, TimestampContainer<Integer>> cluster = CrdtStorageCluster.create(partitions, crdtFunction)
				.withReplicationCount(replicationCount);

		await(CrdtRepartitionController.create(cluster, localPartitionId).repartition());

		Map<CrdtData<String, TimestampContainer<Integer>>, Integer> result = new HashMap<>();

		clients.values().forEach(v -> v.iterator()
				.forEachRemaining(x -> result.compute(x, ($, count) -> count == null ? 1 : (count + 1))));

		assertEquals(new HashSet<>(data), result.keySet());
		for (Integer count : result.values()) {
			assertEquals(replicationCount, count.intValue());
		}
	}
}
