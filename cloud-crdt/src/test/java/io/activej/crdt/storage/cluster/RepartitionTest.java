package io.activej.crdt.storage.cluster;

import io.activej.common.ref.RefInt;
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;

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
		await(StreamSupplier.ofStream(IntStream.range(1, 100).mapToObj(i -> new CrdtData<>("test" + i, TimestampContainer.now(i))))
				.streamTo(StreamConsumer.ofPromise(clients.get("client_0").upload())));

		CrdtStorageCluster<String, String, TimestampContainer<Integer>> cluster = CrdtStorageCluster.create(Eventloop.getCurrentEventloop(), clients, crdtFunction)
				.withReplicationCount(3);

		await(CrdtRepartitionController.create(cluster, "client_0").repartition());

		clients.forEach((k, v) -> {
			System.out.println(k + ":");
			RefInt count = new RefInt(0);
			v.iterator().forEachRemaining(x -> {
				count.inc();
				System.out.println(x);
			});
			System.out.println("Was " + count.get() + " elements");
		});
	}
}
