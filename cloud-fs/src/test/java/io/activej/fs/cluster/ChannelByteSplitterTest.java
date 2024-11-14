package io.activej.fs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.MemSize;
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.process.transformer.ChannelTransformers;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.fs.cluster.FileSystemPartitions.LOCAL_EXCEPTION;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class ChannelByteSplitterTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final ExpectedException EXPECTED_EXCEPTION = new ExpectedException();

	@Test
	public void noFailingOutput() {
		String source = IntStream.range(0, 1000).mapToObj(String::valueOf).collect(joining());
		int nOutputs = 10;
		ChannelByteSplitter splitter = ChannelByteSplitter.create(nOutputs);
		ByteBuf value = wrapUtf8(source);
		splitter.withInput(ChannelSuppliers.ofValue(value)
			.transformWith(ChannelTransformers.chunkBytes(MemSize.of(5), MemSize.of(10))));
		List<String> results = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			splitter.addOutput().set(ChannelConsumers.ofSupplier(supplier -> supplier.toCollector(ByteBufs.collector())
				.then(buf -> {
					results.add(buf.asString(UTF_8));
					return Promise.complete();
				})));
		}
		await(splitter.getProcessCompletion());

		assertEquals(nOutputs, results.size());
		for (String result : results) {
			assertEquals(source, result);
		}
	}

	@Test
	public void failingInputClosesOutputs() {
		String source = IntStream.range(0, 1000).mapToObj(String::valueOf).collect(joining());
		int nOutputs = 10;
		ChannelByteSplitter splitter = ChannelByteSplitter.create(nOutputs);
		ByteBuf value = wrapUtf8(source);
		splitter.withInput(ChannelSuppliers.concat(
			ChannelSuppliers.ofValue(value)
				.transformWith(ChannelTransformers.chunkBytes(MemSize.of(5), MemSize.of(10))),
			failingSupplier()));
		List<ChannelConsumer<ByteBuf>> outputs = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			ChannelConsumer<ByteBuf> output = ChannelConsumers.ofSupplier(supplier -> supplier.toCollector(ByteBufs.collector())
				.then(buf -> {
					buf.recycle();
					return Promise.complete();
				}));
			splitter.addOutput().set(output);
			outputs.add(output);
		}
		Exception exception = awaitException(splitter.getProcessCompletion());

		assertSame(EXPECTED_EXCEPTION, exception);
		for (ChannelConsumer<ByteBuf> output : outputs) {
			assertEquals(LOCAL_EXCEPTION, ((AbstractChannelConsumer<ByteBuf>) output).getException());
		}
	}

	@Test
	public void numberOfSuccessesIsMoreThenRequiredSuccess() {
		String source = IntStream.range(0, 1000).mapToObj(String::valueOf).collect(joining());
		int nOutputs = 10;
		ChannelByteSplitter splitter = ChannelByteSplitter.create(3);
		ByteBuf value = wrapUtf8(source);
		splitter.withInput(ChannelSuppliers.ofValue(value)
			.transformWith(ChannelTransformers.chunkBytes(MemSize.of(5), MemSize.of(10))));
		List<String> results = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			ChannelConsumer<ByteBuf> consumer = i % 3 == 0 ?
				failingConsumer() :
				ChannelConsumers.ofSupplier(supplier -> supplier
					.toCollector(ByteBufs.collector())
					.then(buf -> {
						results.add(buf.asString(UTF_8));
						return Promise.complete();
					}));

			splitter.addOutput().set(consumer);
		}
		await(splitter.getProcessCompletion());

		assertEquals(6, results.size());
		for (String result : results) {
			assertEquals(source, result);
		}
	}

	@Test
	public void numberOfSuccessesIsLessThenRequiredSuccess() {
		String source = IntStream.range(0, 1000).mapToObj(String::valueOf).collect(joining());
		int nOutputs = 10;
		ChannelByteSplitter splitter = ChannelByteSplitter.create(8);
		ByteBuf value = wrapUtf8(source);
		splitter.withInput(ChannelSuppliers.ofValue(value)
			.transformWith(ChannelTransformers.chunkBytes(MemSize.of(5), MemSize.of(10))));
		List<ChannelConsumer<ByteBuf>> outputs = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			if (i % 3 == 0) {
				splitter.addOutput().set(failingConsumer());
			} else {
				ChannelConsumer<ByteBuf> consumer = ChannelConsumers.ofSupplier(supplier -> supplier
					.toCollector(ByteBufs.collector())
					.then(buf -> {
						buf.recycle();
						return Promise.complete();
					}));
				splitter.addOutput().set(consumer);
				outputs.add(consumer);
			}

		}
		Exception exception = awaitException(splitter.getProcessCompletion());

		assertThat(exception.getMessage(), containsString("Not enough successes"));
		assertEquals(6, outputs.size());
		for (ChannelConsumer<ByteBuf> output : outputs) {
			assertSame(LOCAL_EXCEPTION, ((AbstractChannelConsumer<ByteBuf>) output).getException());
		}
	}

	private static ChannelSupplier<ByteBuf> failingSupplier() {
		return ChannelSuppliers.ofException(EXPECTED_EXCEPTION);
	}

	private static ChannelConsumer<ByteBuf> failingConsumer() {
		return ChannelConsumers.ofException(EXPECTED_EXCEPTION);
	}

}
