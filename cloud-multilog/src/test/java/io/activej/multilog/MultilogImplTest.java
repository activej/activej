package io.activej.multilog;

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

import java.util.List;

import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

public class MultilogImplTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		await(StreamSupplierWithResult.ofPromise(
				multilog.read(testPartition, new LogFile("", 0), 0, null))
				.getSupplier()
				.streamTo(listConsumer));

		List<String> list = await(listConsumer.getResult());
		assertEquals(values, list);
	}

}
