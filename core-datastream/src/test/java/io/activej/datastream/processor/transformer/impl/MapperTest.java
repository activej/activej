package io.activej.datastream.processor.transformer.impl;

import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.datastream.supplier.StreamSuppliers.concat;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class MapperTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testFunction() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
		StreamTransformer<Integer, Integer> mapper = new Mapper<>(input -> input * input);

		await(supplier.transformWith(mapper)
				.streamTo(consumer.transformWith(oneByOne())));

		assertEquals(List.of(1, 4, 9), consumer.getList());

		assertEndOfStream(supplier);
		assertEndOfStream(mapper);
		assertEndOfStream(consumer);
	}

	@Test
	public void testFunctionConsumerError() {
		StreamTransformer<Integer, Integer> mapper = new Mapper<>(input -> input * input);

		List<Integer> list = new ArrayList<>();
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValues(1, 2, 3);
		StreamConsumer<Integer> consumer = ToListStreamConsumer.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(source1.transformWith(mapper)
				.streamTo(consumer
						.transformWith(decorate(promise -> promise.then(
								item -> item == 2 * 2 ? Promise.ofException(exception) : Promise.of(item))))));

		assertSame(exception, e);
		assertEquals(List.of(1, 4), list);

		assertClosedWithError(source1);
		assertClosedWithError(consumer);
		assertClosedWithError(mapper);
	}

	@Test
	public void testFunctionSupplierError() {
		StreamTransformer<Integer, Integer> mapper = new Mapper<>(input -> input * input);

		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<Integer> supplier = concat(
				StreamSuppliers.ofValues(1, 2, 3),
				StreamSuppliers.ofValues(4, 5, 6),
				StreamSuppliers.closingWithError(exception)
		);

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		Exception e = awaitException(supplier.transformWith(mapper)
				.streamTo(consumer));

		assertSame(exception, e);
		assertEquals(List.of(1, 4, 9, 16, 25, 36), consumer.getList());

		assertClosedWithError(consumer);
		assertClosedWithError(mapper);
	}

	@Test
	public void testMappedConsumer() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
		StreamTransformer<Integer, Integer> mapper = new Mapper<>(input -> input * input);

		StreamConsumer<Integer> mappedConsumer = consumer
				.transformWith(mapper)
				.transformWith(oneByOne());

		await(supplier.streamTo(mappedConsumer));

		assertEquals(List.of(1, 4, 9), consumer.getList());

		assertEndOfStream(supplier);
		assertEndOfStream(mapper);
		assertEndOfStream(consumer);
	}

	@Test
	public void testConsecutiveMappers() {
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
		StreamTransformer<Integer, Integer> squareMapper = new Mapper<>(input -> input * input);
		StreamTransformer<Integer, Integer> doubleMapper = new Mapper<>(input -> input * 2);
		StreamTransformer<Integer, Integer> mul10Mapper = new Mapper<>(input -> input * 10);

		await(supplier.transformWith(squareMapper).transformWith(doubleMapper)
				.streamTo(consumer.transformWith(mul10Mapper).transformWith(randomlySuspending())));

		assertEquals(List.of(20, 80, 180, 320, 500, 720), consumer.getList());

		assertEndOfStream(supplier);
		assertEndOfStream(squareMapper);
		assertEndOfStream(doubleMapper);
		assertEndOfStream(mul10Mapper);
		assertEndOfStream(consumer);
	}
}
