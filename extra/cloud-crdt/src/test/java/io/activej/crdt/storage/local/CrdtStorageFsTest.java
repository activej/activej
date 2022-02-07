package io.activej.crdt.storage.local;

import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileMetadata;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.activej.common.Utils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.BinarySerializers.*;
import static java.util.Collections.emptySet;
import static org.junit.Assert.*;

public final class CrdtStorageFsTest {
	private static final CrdtFunction<TimestampContainer<Set<Integer>>> CRDT_FUNCTION = TimestampContainer.createCrdtFunction(CrdtStorageFsTest::union);
	private static final CrdtDataSerializer<String, TimestampContainer<Set<Integer>>> SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(ofSet(INT_SERIALIZER)));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private LocalActiveFs fsClient;
	private CrdtStorageFs<String, TimestampContainer<Set<Integer>>> client;

	@Before
	public void setup() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		fsClient = LocalActiveFs.create(eventloop, Executors.newCachedThreadPool(), temporaryFolder.newFolder().toPath());
		client = CrdtStorageFs.create(eventloop, fsClient, SERIALIZER, CRDT_FUNCTION);
		await(fsClient.start());
		await(client.start());
	}

	@Test
	public void testEmptyUpload() {
		await(StreamSupplier.<CrdtData<String, TimestampContainer<Set<Integer>>>>of().streamTo(StreamConsumer.ofPromise(client.upload())));
		assertTrue(await(fsClient.list("**")).isEmpty());
	}

	@Test
	public void testEmptyRemove() {
		await(StreamSupplier.<String>of().streamTo(StreamConsumer.ofPromise(client.remove())));
		assertTrue(await(fsClient.list("**")).isEmpty());
	}

	@Test
	public void testEmptyConsolidation() {
		await(client.consolidate());
		assertTrue(await(fsClient.list("**")).isEmpty());
	}

	@Test
	public void testConsolidation() {
		await(StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("1_test_1", TimestampContainer.now(setOf(1, 2, 3))),
						new CrdtData<>("1_test_2", TimestampContainer.now(setOf(2, 3, 7))),
						new CrdtData<>("1_test_3", TimestampContainer.now(setOf(78, 2, 3))),
						new CrdtData<>("12_test_1", TimestampContainer.now(setOf(123, 124, 125))),
						new CrdtData<>("12_test_2", TimestampContainer.now(setOf(12)))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("2_test_1", TimestampContainer.now(setOf(1, 2, 3))),
						new CrdtData<>("2_test_2", TimestampContainer.now(setOf(2, 3, 4))),
						new CrdtData<>("2_test_3", TimestampContainer.now(setOf(0, 1, 2))),
						new CrdtData<>("12_test_1", TimestampContainer.now(setOf(123, 542, 125, 2))),
						new CrdtData<>("12_test_2", TimestampContainer.now(setOf(12, 13)))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));

		Map<String, FileMetadata> listBefore = await(fsClient.list("**"));
		System.out.println(listBefore);
		assertEquals(2, listBefore.size());

		await(client.consolidate());

		Map<String, FileMetadata> listAfter = await(fsClient.list("**"));
		System.out.println(listAfter);
		assertEquals(1, listAfter.size());
		assertFalse(listBefore.containsKey(first(listAfter.keySet())));
	}

	@Test
	public void testTombstoneConsolidation() {
		await(StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("a", TimestampContainer.now(setOf(1, 2, 3))),
						new CrdtData<>("b", TimestampContainer.now(setOf(2, 3, 7))),
						new CrdtData<>("c", TimestampContainer.now(setOf(78, 2, 3))),
						new CrdtData<>("d", TimestampContainer.now(setOf(123, 124, 125))),
						new CrdtData<>("e", TimestampContainer.now(setOf(12)))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.ofStream(Stream.of("a")).streamTo(StreamConsumer.ofPromise(client.remove())));
		await(StreamSupplier.ofStream(Stream.of("b")).streamTo(StreamConsumer.ofPromise(client.remove())));
		await(StreamSupplier.ofStream(Stream.of("c")).streamTo(StreamConsumer.ofPromise(client.remove())));
		await(StreamSupplier.ofStream(Stream.of("d")).streamTo(StreamConsumer.ofPromise(client.remove())));

		List<CrdtData<String, TimestampContainer<Set<Integer>>>> downloadedBefore = await(client.download().then(StreamSupplier::toList));
		assertEquals(1, downloadedBefore.size());
		assertEquals("e", downloadedBefore.get(0).getKey());

		Map<String, FileMetadata> tombstonesBefore = await(fsClient.list(".tombstones/**"));
		System.out.println(tombstonesBefore);
		assertEquals(4, tombstonesBefore.size());

		await(client.consolidate());

		Map<String, FileMetadata> tombstonesAfter = await(fsClient.list(".tombstones/**"));
		System.out.println(tombstonesAfter);
		assertEquals(1, tombstonesAfter.size());
		String consolidatedTombstone = first(tombstonesAfter.keySet());
		assertFalse(tombstonesBefore.containsKey(consolidatedTombstone));

		List<CrdtData<String, TimestampContainer<Set<Integer>>>> downloadedAfter = await(client.download().then(StreamSupplier::toList));
		assertEquals(1, downloadedAfter.size());
		assertEquals("e", downloadedAfter.get(0).getKey());
	}

	@Test
	public void pickFilesForConsolidation() {
		testPickFilesForConsolidation(
				setOf("a", "c", "e"),
				mapOf(
						"a", 12,
						"b", 120,
						"c", 53,
						"d", 348,
						"e", 97)
				);
		testPickFilesForConsolidation(
				setOf("a", "c"),
				mapOf(
						"a", 120,
						"b", 12,
						"c", 530
				));
		testPickFilesForConsolidation(
				setOf("b", "d"),
				mapOf(
						"a", 120,
						"b", 12,
						"c", 530,
						"d", 43
				));
		testPickFilesForConsolidation(
				emptySet(),
				mapOf(
						"a", 120,
						"b", 12,
						"c", 5,
						"d", 4345
				));
	}

	private static void testPickFilesForConsolidation(Set<String> expected, Map<String, Integer> fileToSizeMap) {
		Map<String, FileMetadata> files = transformMap(fileToSizeMap, size -> FileMetadata.of(size, 0));
		Map<String, FileMetadata> filesForConsolidation = CrdtStorageFs.pickFilesForConsolidation(files);

		assertEquals(expected, filesForConsolidation.keySet());
	}

	private static Set<Integer> union(Set<Integer> first, Set<Integer> second) {
		Set<Integer> res = new HashSet<>(Math.max((int) ((first.size() + second.size()) / .75f) + 1, 16));
		res.addAll(first);
		res.addAll(second);
		return res;
	}
}
