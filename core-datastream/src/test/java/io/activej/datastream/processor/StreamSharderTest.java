package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.function.ToIntFunction;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamSharderTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();
	private static final ToIntFunction<Integer> SHARDER = object -> (object.hashCode() & Integer.MAX_VALUE) % 2;

	@Test
	public void test1() {
		StreamSplitter<Integer, Integer> streamSharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[SHARDER.applyAsInt(item)].accept(item));

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

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

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

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

		StreamSupplier<Integer> source = StreamSupplier.of(1, 2, 3, 4);

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();
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

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(1),
				StreamSupplier.of(2),
				StreamSupplier.of(3),
				StreamSupplier.closingWithError(exception)
		);

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

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
