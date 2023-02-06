package io.activej.datastream.processor.transformer.sort;

import io.activej.common.MemSize;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.datastream.processor.transformer.sort.FailingStubStreamSorterStorage.STORAGE_EXCEPTION;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static org.junit.Assert.*;

public final class StreamSorterTest {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufPool = new ByteBufRule();

	private static final FrameFormat FRAME_FORMAT = FrameFormats.sizePrefixed();

	@Test
	public void testStreamStorage() {
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6, 7);
		//		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(111);

		Executor executor = Executors.newSingleThreadExecutor();
		StreamSorterStorage<Integer> storage = StreamSorterStorage.builder(executor, INT_SERIALIZER, FRAME_FORMAT, tempFolder.getRoot().toPath())
				.withWriteBlockSize(MemSize.of(64))
				.build();

		StreamConsumer<Integer> writer1 = storage.writeStream(1);
//		StreamConsumer<Integer> writer2 = storage.writeStream(2);
		source1.streamTo(writer1);
//		source2.streamTo(writer2);

		await();

		assertEndOfStream(source1);
//		assertEndOfStream(source2);

		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
//		StreamConsumerToListStreamConsumer<Integer> consumer2 = StreamConsumerToListStreamConsumer.create();
		storage.readStream(1).streamTo(consumer1.transformWith(oneByOne()));
//		storage.readStream(2).streamTo(consumer2.with(TestStreamConsumers.randomlySuspending()));
		await();

		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), consumer1.getList());
//		assertEquals(List.of(111), consumer2.getList());

		storage.cleanup(List.of(1, 2));
	}

	@Test
	public void test() throws Exception {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = Executors.newSingleThreadExecutor();
		IStreamSorterStorage<Integer> storage = StreamSorterStorage.create(executor, INT_SERIALIZER, FRAME_FORMAT, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(storage, Function.identity(), Integer::compareTo, true, 2);

		ToListStreamConsumer<Integer> consumerToList = ToListStreamConsumer.create();

		await(source.transformWith(sorter)
				.streamTo(consumerToList.transformWith(randomlySuspending())));

		assertEquals(List.of(1, 2, 3, 4, 5), consumerToList.getList());
		assertEndOfStream(source, consumerToList);
		assertEndOfStream(sorter);
	}

	@Test
	public void testErrorOnConsumer() throws IOException {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = Executors.newSingleThreadExecutor();
		IStreamSorterStorage<Integer> storage = StreamSorterStorage.create(executor, INT_SERIALIZER, FRAME_FORMAT, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 2);

		List<Integer> list = new ArrayList<>();
		StreamConsumer<Integer> consumer = ToListStreamConsumer.create(list);
		ExpectedException exception = new ExpectedException();

		Exception e = awaitException(
				source.streamTo(sorter.getInput()),
				sorter.getOutput()
						.streamTo(consumer
								.transformWith(decorate(promise -> promise.then(
										item -> item == 5 ? Promise.ofException(exception) : Promise.of(item)))))
		);

		assertSame(exception, e);
		assertClosedWithError(source);
		assertClosedWithError(exception, consumer);
		assertClosedWithError(exception, sorter);
	}

	@Test
	public void testErrorOnSupplier() throws IOException {
		Executor executor = Executors.newSingleThreadExecutor();
		ExpectedException exception = new ExpectedException();

		StreamSupplier<Integer> source = StreamSuppliers.concat(
				StreamSuppliers.ofValues(3, 1, 3, 2),
				StreamSuppliers.closingWithError(exception)
		);

		IStreamSorterStorage<Integer> storage = StreamSorterStorage.create(executor, INT_SERIALIZER, FRAME_FORMAT, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 10);

		ToListStreamConsumer<Integer> consumerToList = ToListStreamConsumer.create();

		Exception e = awaitException(source.transformWith(sorter)
				.streamTo(consumerToList));

		assertSame(exception, e);
		assertEquals(0, consumerToList.getList().size());
		assertClosedWithError(exception, source, consumerToList);
		assertClosedWithError(exception, sorter);
	}

	@Test
	public void testCleanup() throws IOException {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(6, 5, 4, 3, 2, 1);

		Executor executor = Executors.newSingleThreadExecutor();
		Path storagePath = tempFolder.newFolder().toPath();
		IStreamSorterStorage<Integer> storage = StreamSorterStorage.create(executor, INT_SERIALIZER, FRAME_FORMAT, storagePath);
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(storage, Function.identity(), Integer::compareTo, true, 0);

		StreamConsumer<Integer> consumer = StreamConsumers.skip();

		try (Stream<Path> contents = Files.list(storagePath)) {
			assertFalse(contents.findAny().isPresent());
		}

		Promise<Void> inputPromise = source.streamTo(sorter.getInput());

		// wait some time till files are actually created
		await(Promise.complete().async());

		try (Stream<Path> contents = Files.list(storagePath)) {
			assertEquals(6, contents.count());
		}

		await(Promises.all(inputPromise, sorter.getOutput().streamTo(consumer.transformWith(randomlySuspending())))
				.whenResult(() -> {
					try (Stream<Path> contents = Files.list(storagePath)) {
						assertFalse(contents.findAny().isPresent());
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				}));
	}

	@Test
	public void testErrorsOnStorage() throws IOException {
		FailingStubStreamSorterStorage<Integer> failingNewPartitionStorage = FailingStubStreamSorterStorage.<Integer>create().withFailNewPartition();
		doTestFailingStorage(failingNewPartitionStorage, (streamPromise, sorter, supplier, consumerToList) -> {
			Exception exception = awaitException(streamPromise);
			assertSame(STORAGE_EXCEPTION, exception);
			assertClosedWithError(STORAGE_EXCEPTION, sorter);
			assertClosedWithError(STORAGE_EXCEPTION, supplier, consumerToList);
			assertTrue(consumerToList.getList().isEmpty());
		});

		FailingStubStreamSorterStorage<Integer> failingWriteStorage = FailingStubStreamSorterStorage.<Integer>create().withFailWrite();
		doTestFailingStorage(failingWriteStorage, (streamPromise, sorter, supplier, consumerToList) -> {
			Exception exception = awaitException(streamPromise);
			assertSame(STORAGE_EXCEPTION, exception);
			assertClosedWithError(STORAGE_EXCEPTION, sorter);
			assertClosedWithError(STORAGE_EXCEPTION, supplier, consumerToList);
			assertTrue(consumerToList.getList().isEmpty());
		});

		FailingStubStreamSorterStorage<Integer> failingReadStorage = FailingStubStreamSorterStorage.<Integer>create().withFailRead();
		doTestFailingStorage(failingReadStorage, (streamPromise, sorter, supplier, consumerToList) -> {
			Exception exception = awaitException(streamPromise);
			assertSame(STORAGE_EXCEPTION, exception);
			assertClosedWithError(STORAGE_EXCEPTION, sorter);
			assertClosedWithError(supplier);
			assertClosedWithError(STORAGE_EXCEPTION, consumerToList);
			assertTrue(consumerToList.getList().isEmpty());
		});

		FailingStubStreamSorterStorage<Integer> failingCleanup = FailingStubStreamSorterStorage.<Integer>create().withFailCleanup();
		doTestFailingStorage(failingCleanup, (streamPromise, sorter, supplier, consumerToList) -> {
			await(streamPromise);
			assertEndOfStream(sorter);
			assertEndOfStream(supplier, consumerToList);
			assertEquals(List.of(1, 2, 3, 4, 5), consumerToList.getList());
		});
	}

	private void doTestFailingStorage(FailingStubStreamSorterStorage<Integer> failingStorage, StreamSorterValidator<Integer, Integer> validator) throws IOException {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = Executors.newSingleThreadExecutor();
		Path path = tempFolder.newFolder().toPath();
		failingStorage.setStorage(StreamSorterStorage.create(executor, INT_SERIALIZER, FRAME_FORMAT, path));
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(failingStorage, Function.identity(), Integer::compareTo, true, 2);

		ToListStreamConsumer<Integer> consumerToList = ToListStreamConsumer.create();

		Promise<Void> streamPromise = source.transformWith(sorter)
				.streamTo(consumerToList.transformWith(randomlySuspending()));

		validator.validate(streamPromise, sorter, source, consumerToList);

		try (Stream<Path> list = Files.list(path)) {
			assertEquals(failingStorage.failCleanup, list.findAny().isPresent());
		}
	}

	private interface StreamSorterValidator<K, T> {
		void validate(Promise<Void> streamPromise, StreamSorter<K, T> sorter, StreamSupplier<T> supplier, ToListStreamConsumer<T> consumerToList);
	}

}
