package io.activej.multilog;

import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.process.ChannelByteRanger;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.StreamSupplierWithResult;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.serializer.BinarySerializers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultilogImplTest {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testConsumer() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Multilog<String> multilog = MultilogImpl.create(eventloop,
				LocalActiveFs.create(eventloop, newSingleThreadExecutor(), temporaryFolder.getRoot().toPath()),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values)
				.streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		assertEquals(values, readLog(multilog, testPartition));
	}

	@Test
	public void testIgnoringMalformedLogs() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Path storage = temporaryFolder.getRoot().toPath();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), storage);
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs, BinarySerializers.UTF8_SERIALIZER, NAME_PARTITION_REMAINDER_SEQ)
				.withIgnoreMalformedLogs(true);

		String testPartition1 = "partition1";
		String testPartition2 = "partition2";
		String testPartition3 = "partition3";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(testPartition1)));
		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(testPartition2)));
		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(testPartition3)));

		// malformed data
		await(fs.list("*" + testPartition1 + "*")
				.then(map -> {
					String filename = first(map.keySet());
					return ChannelSupplier.of(wrapUtf8("MALFORMED")).streamTo(fs.upload(filename));
				}));

		// Truncated data
		await(fs.list("*" + testPartition2 + "*")
				.then(map -> {
					String filename = first(map.keySet());
					return fs.download(filename)
							.then(supplier -> supplier
									.transformWith(ChannelByteRanger.range(0, map.get(filename).getSize() / 2))
									.streamTo(fs.upload(filename)));
				}));

		// Unexpected data
		await(fs.list("*" + testPartition3 + "*")
				.then(map -> {
					String filename = first(map.keySet());
					return fs.download(filename)
							.then(supplier -> ChannelSuppliers.concat(supplier, ChannelSupplier.of(wrapUtf8("UNEXPECTED DATA")))
									.streamTo(fs.upload(filename)));
				}));

		assertTrue(readLog(multilog, testPartition1).isEmpty());
		assertTrue(readLog(multilog, testPartition2).isEmpty());
		assertEquals(values, readLog(multilog, testPartition3));
	}

	@Test
	public void testIgnoringReadsPastFileSize() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Path storage = temporaryFolder.getRoot().toPath();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), storage);
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs, BinarySerializers.UTF8_SERIALIZER, NAME_PARTITION_REMAINDER_SEQ)
				.withIgnoreMalformedLogs(true);

		String testPartition = "partition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(testPartition)));

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		await(fs.list("*" + testPartition + "*")
				.then(map -> {
					PartitionAndFile partitionAndFile = NAME_PARTITION_REMAINDER_SEQ.parse(first(map.keySet()));
					assert partitionAndFile != null;
					LogFile logFile = partitionAndFile.getLogFile();
					return StreamSupplierWithResult.ofPromise(
							multilog.read(testPartition, logFile, first(map.values()).getSize()  * 2, null))
							.getSupplier()
							.streamTo(listConsumer);
				}));

		assertTrue(listConsumer.getList().isEmpty());
	}

	private static <T> List<T> readLog(Multilog<T> multilog, String partition) {
		StreamConsumerToList<T> listConsumer = StreamConsumerToList.create();
		await(StreamSupplierWithResult.ofPromise(
				multilog.read(partition, new LogFile("", 0), 0, null))
				.getSupplier()
				.streamTo(listConsumer));

		return listConsumer.getList();
	}

}
