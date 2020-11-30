package io.activej.fs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.common.exception.ExpectedException;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.fs.cluster.FsPartitions.LOCAL_EXCEPTION;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

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
		splitter.withInput(ChannelSupplier.of(wrapUtf8(source))
				.transformWith(ChannelByteChunker.create(MemSize.of(5), MemSize.of(10))));
		List<String> results = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			splitter.addOutput().set(ChannelConsumer.ofSupplier(supplier -> supplier.toCollector(ByteBufQueue.collector())
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
		splitter.withInput(ChannelSuppliers.concat(
				ChannelSupplier.of(wrapUtf8(source))
						.transformWith(ChannelByteChunker.create(MemSize.of(5), MemSize.of(10))),
				failingSupplier()));
		List<ChannelConsumer<ByteBuf>> outputs = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			ChannelConsumer<ByteBuf> output = ChannelConsumer.ofSupplier(supplier -> supplier.toCollector(ByteBufQueue.collector())
					.then(buf -> {
						buf.recycle();
						return Promise.complete();
					}));
			splitter.addOutput().set(output);
			outputs.add(output);
		}
		Throwable exception = awaitException(splitter.getProcessCompletion());

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
		splitter.withInput(ChannelSupplier.of(wrapUtf8(source))
				.transformWith(ChannelByteChunker.create(MemSize.of(5), MemSize.of(10))));
		List<String> results = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			ChannelConsumer<ByteBuf> consumer = i % 3 == 0 ?
					failingConsumer() :
					ChannelConsumer.ofSupplier(supplier -> supplier
							.toCollector(ByteBufQueue.collector())
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
		splitter.withInput(ChannelSupplier.of(wrapUtf8(source))
				.transformWith(ChannelByteChunker.create(MemSize.of(5), MemSize.of(10))));
		List<ChannelConsumer<ByteBuf>> outputs = new ArrayList<>();
		for (int i = 0; i < nOutputs; i++) {
			if (i % 3 == 0) {
				splitter.addOutput().set(failingConsumer());
			} else {
				ChannelConsumer<ByteBuf> consumer = ChannelConsumer.ofSupplier(supplier -> supplier
						.toCollector(ByteBufQueue.collector())
						.then(buf -> {
							buf.recycle();
							return Promise.complete();
						}));
				splitter.addOutput().set(consumer);
				outputs.add(consumer);
			}

		}
		Throwable throwable = awaitException(splitter.getProcessCompletion());

		assertThat(throwable.getMessage(), containsString("Not enough successes"));
		assertEquals(6, outputs.size());
		for (ChannelConsumer<ByteBuf> output : outputs) {
			assertSame(LOCAL_EXCEPTION, ((AbstractChannelConsumer<ByteBuf>) output).getException());
		}
	}

	private static ChannelSupplier<ByteBuf> failingSupplier() {
		return ChannelSupplier.ofException(EXPECTED_EXCEPTION);
	}

	private static ChannelConsumer<ByteBuf> failingConsumer() {
		return ChannelConsumer.ofException(EXPECTED_EXCEPTION);
	}

}
