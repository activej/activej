package io.activej.crdt.storage.local;

import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class TestCrdtLocalFileConsolidation {
	private LocalActiveFs fsClient;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		fsClient = LocalActiveFs.create(Eventloop.getCurrentEventloop(), newSingleThreadExecutor(), temporaryFolder.newFolder().toPath());
	}

	private Set<Integer> union(Set<Integer> first, Set<Integer> second) {
		Set<Integer> res = new HashSet<>(Math.max((int) ((first.size() + second.size()) / .75f) + 1, 16));
		res.addAll(first);
		res.addAll(second);
		return res;
	}

	@Test
	public void test() {
		CrdtFunction<TimestampContainer<Set<Integer>>> crdtFunction = TimestampContainer.createCrdtFunction(this::union);

		CrdtDataSerializer<String, TimestampContainer<Set<Integer>>> serializer =
				new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(ofSet(INT_SERIALIZER)));
		CrdtStorageFs<String, TimestampContainer<Set<Integer>>> client = CrdtStorageFs.create(Eventloop.getCurrentEventloop(), fsClient, serializer, crdtFunction);

		await(StreamSupplier.ofStream(Stream.of(
				new CrdtData<>("1_test_1", TimestampContainer.now(set(1, 2, 3))),
				new CrdtData<>("1_test_2", TimestampContainer.now(set(2, 3, 7))),
				new CrdtData<>("1_test_3", TimestampContainer.now(set(78, 2, 3))),
				new CrdtData<>("12_test_1", TimestampContainer.now(set(123, 124, 125))),
				new CrdtData<>("12_test_2", TimestampContainer.now(set(12)))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.ofStream(Stream.of(
				new CrdtData<>("2_test_1", TimestampContainer.now(set(1, 2, 3))),
				new CrdtData<>("2_test_2", TimestampContainer.now(set(2, 3, 4))),
				new CrdtData<>("2_test_3", TimestampContainer.now(set(0, 1, 2))),
				new CrdtData<>("12_test_1", TimestampContainer.now(set(123, 542, 125, 2))),
				new CrdtData<>("12_test_2", TimestampContainer.now(set(12, 13)))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));

		System.out.println(await(fsClient.list("**")));
		await(client.consolidate());
		System.out.println(await(fsClient.list("**")));
	}
}
