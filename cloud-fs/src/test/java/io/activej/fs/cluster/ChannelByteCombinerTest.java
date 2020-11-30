package io.activej.fs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.ExpectedException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class ChannelByteCombinerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final ExpectedException EXPECTED_EXCEPTION = new ExpectedException();

	private ChannelByteCombiner combiner;

	@Before
	public void setUp() {
		combiner = ChannelByteCombiner.create();
	}

	@Test
	public void exceptionOnStart() {
		combiner.addInput().set(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3")).async());
		combiner.addInput().set(failing().async());
		combiner.addInput().set(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3"), wrapUtf8("4"), wrapUtf8("5"),
				wrapUtf8("6"), wrapUtf8("7"), wrapUtf8("8")).async());
		combiner.addInput().set(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3"), wrapUtf8("4"), wrapUtf8("5")).async());

		String result = await(combiner.getOutput().getSupplier().toCollector(ByteBufQueue.collector())).asString(UTF_8);

		assertEquals("12345678", result);
	}

	@Test
	public void exceptionInProcess() {
		combiner.addInput().set(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3")).async());
		combiner.addInput().set(ChannelSuppliers.concat(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2")), failing()).async());
		combiner.addInput().set(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3"), wrapUtf8("4"), wrapUtf8("5"),
				wrapUtf8("6"), wrapUtf8("7"), wrapUtf8("8")).async());
		combiner.addInput().set(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3"), wrapUtf8("4"), wrapUtf8("5")).async());

		String result = await(combiner.getOutput().getSupplier().toCollector(ByteBufQueue.collector())).asString(UTF_8);

		assertEquals("12345678", result);
	}

	@Test
	public void allExceptionsOnStart() {
		int inputSize = 10;
		for (int i = 0; i < inputSize; i++) {
			combiner.addInput().set(failing());
		}

		Throwable exception = awaitException(combiner.getOutput().getSupplier().toCollector(ByteBufQueue.collector()));

		assertSame(EXPECTED_EXCEPTION, exception);
	}

	@Test
	public void allExceptionsOnInProcess() {
		combiner.addInput().set(ChannelSuppliers.concat(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3")), failing()).async());
		combiner.addInput().set(ChannelSuppliers.concat(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2")), failing()).async());
		combiner.addInput().set(ChannelSuppliers.concat(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3"), wrapUtf8("4"), wrapUtf8("5"),
				wrapUtf8("6"), wrapUtf8("7"), wrapUtf8("8")), failing()).async());
		combiner.addInput().set(ChannelSuppliers.concat(ChannelSupplier.of(wrapUtf8("1"), wrapUtf8("2"), wrapUtf8("3"), wrapUtf8("4"), wrapUtf8("5")),
				failing()).async());

		Throwable exception = awaitException(combiner.getOutput().getSupplier().toCollector(ByteBufQueue.collector()));

		assertSame(EXPECTED_EXCEPTION, exception);
	}

	private static ChannelSupplier<ByteBuf> failing() {
		return ChannelSupplier.ofException(EXPECTED_EXCEPTION);
	}

}
