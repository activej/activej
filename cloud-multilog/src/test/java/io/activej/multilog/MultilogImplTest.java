package io.activej.multilog;

import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.process.ChannelByteRanger;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.StreamSupplierWithResult;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.serializer.BinarySerializers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
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
				LZ4FrameFormat.create(),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
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
				LZ4FrameFormat.create(),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ)
				.withBufferSize(1);

		String partition = "partition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values).streamTo(multilog.write(partition)));

		// Truncated data
		await(fs.list("*" + partition + "*")
				.then(map -> {
					String filename = first(map.keySet());
					return fs.download(filename)
							.then(supplier -> supplier
									.transformWith(ChannelByteRanger.range(0, 32))
									.streamTo(fs.upload(filename)));
				}));

		assertEquals(asList("test1", "test2"), readLog(multilog, partition));
	}

	@Test
	public void testIgnoringMalformedLogs() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Path storage = temporaryFolder.getRoot().toPath();
		LocalActiveFs fs = LocalActiveFs.create(eventloop, newSingleThreadExecutor(), storage);
		await(fs.start());
		Multilog<String> multilog = MultilogImpl.create(eventloop, fs,
				LZ4FrameFormat.create(),
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
				LZ4FrameFormat.create(),
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

	private static final LogNamingScheme EACH_ITEM_NEW_FILE_SCHEME = new LogNamingScheme() {
		@Override
		public String path(String logPartition, LogFile logFile) {
			return NAME_PARTITION_REMAINDER_SEQ.path(logPartition, logFile);
		}

		@Override
		public @Nullable PartitionAndFile parse(String name) {
			return NAME_PARTITION_REMAINDER_SEQ.parse(name);
		}

		@Override
		public LogFile format(long currentTimeMillis) {
			LogFile format = NAME_PARTITION_REMAINDER_SEQ.format(currentTimeMillis);
			return new LogFile(format.getName() + currentTimeMillis, format.getRemainder());
		}

		@Override
		public String getListGlob(String logPartition) {
			return NAME_PARTITION_REMAINDER_SEQ.getListGlob(logPartition);
		}
	};

	private static <T> List<T> readLog(Multilog<T> multilog, String partition) {
		StreamConsumerToList<T> listConsumer = StreamConsumerToList.create();
		await(StreamSupplierWithResult.ofPromise(
				multilog.read(partition, new LogFile("", 0), 0, null))
				.getSupplier()
				.streamTo(listConsumer));

		return listConsumer.getList();
	}

}
