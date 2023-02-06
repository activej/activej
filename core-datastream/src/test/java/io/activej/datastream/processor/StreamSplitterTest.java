package io.activej.datastream.processor;

import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSplitterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final ToIntFunction<Integer> SHARDER = object -> (object.hashCode() & Integer.MAX_VALUE) % 2;

	@Test
	public void test() {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3);
		StreamSplitter<Integer, Integer> streamConcat = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});
		ToListStreamConsumer<Integer> consumerToList1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumerToList2 = ToListStreamConsumer.create();

		await(
				source.streamTo(streamConcat.getInput()),
				streamConcat.newOutput().streamTo(consumerToList1.transformWith(oneByOne())),
				streamConcat.newOutput().streamTo(consumerToList2.transformWith(oneByOne()))
		);

		assertEquals(List.of(1, 2, 3), consumerToList1.getList());
		assertEquals(List.of(1, 2, 3), consumerToList2.getList());
		assertEndOfStream(source);
		assertEndOfStream(streamConcat.getInput());
		assertSuppliersEndOfStream(streamConcat.getOutputs());
	}

	@Test
	public void testConsumerDisconnectWithError() {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3, 4, 5);
		StreamSplitter<Integer, Integer> streamSplitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		ToListStreamConsumer<Integer> consumerToList1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumerToList2 = ToListStreamConsumer.create();

		ToListStreamConsumer<Integer> badConsumer = ToListStreamConsumer.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(
				source.streamTo(streamSplitter.getInput()),
				streamSplitter.newOutput()
						.streamTo(consumerToList1
								.transformWith(oneByOne())),
				streamSplitter.newOutput()
						.streamTo(badConsumer
								.transformWith(decorate(promise ->
										promise.then(item -> Promise.ofException(exception))))
								.transformWith(oneByOne())
						),
				streamSplitter.newOutput()
						.streamTo(consumerToList2
								.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		// assertEquals(3, consumerToList1.getList().size());
		// assertEquals(3, consumerToList1.getList().size());
		// assertEquals(3, toBadList.size());

		assertClosedWithError(source);
		assertClosedWithError(streamSplitter.getInput());
		assertSuppliersClosedWithError(streamSplitter.getOutputs());
	}

	@Test
	public void testSupplierDisconnectWithError() {
		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<Integer> source = StreamSuppliers.concat(
				StreamSuppliers.ofValue(1),
				StreamSuppliers.ofValue(2),
				StreamSuppliers.ofValue(3),
				StreamSuppliers.closingWithError(exception)
		);

		StreamSplitter<Integer, Integer> splitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumer2 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumer3 = ToListStreamConsumer.create();

		Exception e = awaitException(
				source.streamTo(splitter.getInput()),
				splitter.newOutput().streamTo(consumer1.transformWith(oneByOne())),
				splitter.newOutput().streamTo(consumer2.transformWith(oneByOne())),
				splitter.newOutput().streamTo(consumer3.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		assertEquals(3, consumer1.getList().size());
		assertEquals(3, consumer2.getList().size());
		assertEquals(3, consumer3.getList().size());

		assertClosedWithError(splitter.getInput());
		assertSuppliersClosedWithError(splitter.getOutputs());
	}

	@Test
	public void testNoOutputs() {
		StreamSplitter<Integer, Integer> splitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		await(StreamSuppliers.ofValues(1, 2, 3, 4).streamTo(splitter.getInput()));
	}

	@Test
	public void testSuspendSideEffect() {
		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3);
		StreamSplitter<Integer, Integer> splitter = StreamSplitter.create(
				(item, acceptors) -> {
					for (StreamDataAcceptor<Integer> acceptor : acceptors) {
						acceptor.accept(item);
					}
				});

		List<Integer> list = new ArrayList<>();
		AbstractStreamConsumer<Integer> consumer = new AbstractStreamConsumer<>() {
			@Override
			protected void onStarted() {
				resume(list::add);
			}

			@Override
			protected void onEndOfStream() {
				acknowledge();
			}
		};

		await(
				source.streamTo(splitter.getInput()),
				splitter.newOutput().streamTo(new AbstractStreamConsumer<>() {
					@Override
					protected void onStarted() {
						resume(item -> {
							if (item == 2) {
								consumer.suspend();
								reactor.post(() -> consumer.resume(list::add));
							}
						});
					}

					@Override
					protected void onEndOfStream() {
						acknowledge();
					}
				}),
				splitter.newOutput().streamTo(consumer)
		);

		assertEquals(List.of(1, 2, 3), list);
	}

	@Test
	public void testSharder() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.applyAsInt(item)].accept(item));

		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3, 4);
		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumer2 = ToListStreamConsumer.create();

		await(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1.transformWith(randomlySuspending())),
				streamSharder.newOutput().streamTo(consumer2.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(2, 4), consumer1.getList());
		assertEquals(List.of(1, 3), consumer2.getList());

		assertEndOfStream(source);
		assertEndOfStream(streamSharder.getInput());
		assertEndOfStream(streamSharder.getOutput(0));
		assertEndOfStream(consumer1);
		assertEndOfStream(consumer2);
	}

	@Test
	public void test2() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.applyAsInt(item)].accept(item));

		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3, 4);
		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumer2 = ToListStreamConsumer.create();

		await(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1.transformWith(randomlySuspending())),
				streamSharder.newOutput().streamTo(consumer2.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(2, 4), consumer1.getList());
		assertEquals(List.of(1, 3), consumer2.getList());

		assertEndOfStream(source);
		assertEndOfStream(source);
		assertEndOfStream(streamSharder.getInput());
		assertEndOfStream(streamSharder.getOutput(0));
		assertEndOfStream(consumer1);
		assertEndOfStream(consumer2);
	}

	@Test
	public void testWithError() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.applyAsInt(item)].accept(item));

		StreamSupplier<Integer> source = StreamSuppliers.ofValues(1, 2, 3, 4);

		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumer2 = ToListStreamConsumer.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1),
				streamSharder.newOutput().streamTo(
						consumer2.transformWith(decorate(promise ->
								promise.then(item -> item == 3 ? Promise.ofException(exception) : Promise.of(item))))));

		assertSame(exception, e);
		assertEquals(1, consumer1.getList().size());
		assertEquals(2, consumer2.getList().size());
		assertClosedWithError(source);
		assertClosedWithError(source);
		assertClosedWithError(streamSharder.getInput());
		assertSuppliersClosedWithError(streamSharder.getOutputs());
		assertClosedWithError(consumer1);
		assertClosedWithError(consumer2);
	}

	@Test
	public void testSupplierWithError() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.applyAsInt(item)].accept(item));

		ExpectedException exception = new ExpectedException("Test Exception");

		StreamSupplier<Integer> source = StreamSuppliers.concat(
				StreamSuppliers.ofValue(1),
				StreamSuppliers.ofValue(2),
				StreamSuppliers.ofValue(3),
				StreamSuppliers.closingWithError(exception)
		);

		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> consumer2 = ToListStreamConsumer.create();

		Exception e = awaitException(
				source.streamTo(streamSharder.getInput()),
				streamSharder.newOutput().streamTo(consumer1.transformWith(oneByOne())),
				streamSharder.newOutput().streamTo(consumer2.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		assertEquals(1, consumer1.getList().size());
		assertEquals(2, consumer2.getList().size());

		assertClosedWithError(streamSharder.getInput());
		assertSuppliersClosedWithError(streamSharder.getOutputs());
		assertClosedWithError(consumer1);
		assertClosedWithError(consumer2);
	}
}
