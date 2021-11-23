package io.activej.multilog;

import io.activej.common.MemSize;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.process.ChannelByteRanger;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.csp.process.frames.LZ4LegacyFrameFormat;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.StreamSupplierWithResult;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileMetadata;
import io.activej.fs.LocalActiveFs;
import io.activej.serializer.BinarySerializers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.Utils.first;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MultilogImplTest {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public FrameFormat frameFormat;

	@Parameter(2)
	public int endOfStreamBlockSize;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{"LZ4 format", LZ4FrameFormat.create(), 8},
				new Object[]{"Legacy LZ4 format", LZ4LegacyFrameFormat.create(), 21}
		);
	}

	@Test
	public void testConsumer() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), temporaryFolder.getRoot().toPath());
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs, frameFormat, BinarySerializers.UTF8_SERIALIZER, NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values)
				.streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		assertEquals(values, readLog(multilog, testPartition));
	}

	@Test
	public void testIgnoringTruncatedLogs() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Path storage = temporaryFolder.getRoot().toPath();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), storage);
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs,
				frameFormat,
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ)
				.withBufferSize(1);

		String partition = "partition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(partition)));

		// Truncated data
		await(fs.list("*" + partition + "*")
				.then(map -> {
					Entry<String, FileMetadata> entry = first(map.entrySet());
					return fs.download(entry.getKey())
							.then(supplier -> supplier
									.transformWith(ChannelByteRanger.range(0, entry.getValue().getSize() - endOfStreamBlockSize))
									.streamTo(fs.upload(entry.getKey())));
				}));

		assertEquals(values, readLog(multilog, partition));
	}

	@Test
	public void testIgnoringMalformedLogs() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Path storage = temporaryFolder.getRoot().toPath();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), storage);
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs,
				frameFormat,
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ)
				.withIgnoreMalformedLogs(true);

		String partition1 = "partition1";
		String partition2 = "partition2";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(partition1)));
		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(partition2)));

		// malformed data
		await(fs.list("*" + partition1 + "*")
				.then(map -> {
					String filename = first(map.keySet());
					return ChannelSupplier.of(wrapUtf8("MALFORMED")).streamTo(fs.upload(filename));
				}));

		// Unexpected data
		await(fs.list("*" + partition2 + "*")
				.then(map -> {
					String filename = first(map.keySet());
					return fs.download(filename)
							.then(supplier -> ChannelSuppliers.concat(supplier, ChannelSupplier.of(wrapUtf8("UNEXPECTED DATA")))
									.streamTo(fs.upload(filename)));
				}));

		assertTrue(readLog(multilog, partition1).isEmpty());
		assertEquals(values, readLog(multilog, partition2));
	}

	@Test
	public void testIgnoringReadsPastFileSize() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Path storage = temporaryFolder.getRoot().toPath();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), storage);
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs,
				frameFormat,
				BinarySerializers.UTF8_SERIALIZER, NAME_PARTITION_REMAINDER_SEQ)
				.withIgnoreMalformedLogs(true);

		String partition = "partition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(partition)));

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		await(fs.list("*" + partition + "*")
				.then(map -> {
					PartitionAndFile partitionAndFile = NAME_PARTITION_REMAINDER_SEQ.parse(first(map.keySet()));
					assert partitionAndFile != null;
					LogFile logFile = partitionAndFile.getLogFile();
					return StreamSupplierWithResult.ofPromise(
							multilog.read(partition, logFile, first(map.values()).getSize() * 2, null))
							.getSupplier()
							.streamTo(listConsumer);
				}));

		assertTrue(listConsumer.getList().isEmpty());
	}

	@Test
	public void logPositionIsCountedCorrectly() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), temporaryFolder.getRoot().toPath())
				.withReaderBufferSize(MemSize.bytes(1));

		await(fs.start());

		Multilog<String> multilog = MultilogImpl.create(eventloop,
				fs,
				frameFormat,
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ)
				.withBufferSize(MemSize.bytes(1));

		String testPartition = "partition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values)
				.streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		StreamSupplierWithResult<String, LogPosition> supplierWithResult = StreamSupplierWithResult.ofPromise(
				multilog.read(testPartition, new LogFile("", 0), 0, null));

		StreamConsumerToList<String> consumerToList = StreamConsumerToList.create();
		await(supplierWithResult.getSupplier().streamTo(consumerToList));

		assertEquals(values, consumerToList.getList());

		LogPosition pos = await(supplierWithResult.getResult());

		long position = pos.getPosition();

		// check that position does not change on second call
		supplierWithResult = StreamSupplierWithResult.ofPromise(
				multilog.read(testPartition, pos.getLogFile(), position, null));

		consumerToList = StreamConsumerToList.create();
		await(supplierWithResult.getSupplier().streamTo(consumerToList));

		assertTrue(consumerToList.getList().isEmpty());

		assertEquals(position, await(supplierWithResult.getResult()).getPosition());
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
