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

import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertTrue;

public final class CrdtStorageFsTest {
	public static final CrdtFunction<TimestampContainer<Set<Integer>>> CRDT_FUNCTION = TimestampContainer.createCrdtFunction(CrdtStorageFsTest::union);
	public static final CrdtDataSerializer<String, TimestampContainer<Set<Integer>>> SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(ofSet(INT_SERIALIZER)));
	private LocalActiveFs fsClient;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		fsClient = LocalActiveFs.create(Eventloop.getCurrentEventloop(), newSingleThreadExecutor(), temporaryFolder.newFolder().toPath());
		await(fsClient.start());
	}

	private static Set<Integer> union(Set<Integer> first, Set<Integer> second) {
		Set<Integer> res = new HashSet<>(Math.max((int) ((first.size() + second.size()) / .75f) + 1, 16));
		res.addAll(first);
		res.addAll(second);
		return res;
	}

	@Test
	public void testEmptyUpload() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		CrdtStorageFs<String, TimestampContainer<Set<Integer>>> client = CrdtStorageFs.create(eventloop, fsClient, SERIALIZER, CRDT_FUNCTION);
		await(StreamSupplier.<CrdtData<String, TimestampContainer<Set<Integer>>>>of().streamTo(StreamConsumer.ofPromise(client.upload())));
		assertTrue(await(fsClient.list("**")).isEmpty());
	}

	@Test
	public void testEmptyRemove() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		CrdtStorageFs<String, TimestampContainer<Set<Integer>>> client = CrdtStorageFs.create(eventloop, fsClient, SERIALIZER, CRDT_FUNCTION);
		await(StreamSupplier.<String>of().streamTo(StreamConsumer.ofPromise(client.remove())));
		assertTrue(await(fsClient.list("**")).isEmpty());
	}

}
