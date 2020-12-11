package io.activej.csp.process;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.recycle.Recyclers;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public final class AbstractCommunicatingProcessTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final MalformedDataException ERROR = new MalformedDataException("Test Error");

	private final int size = 10;
	private final List<ByteBuf> actualData = new ArrayList<>();
	private final PassThroughProcess[] processes = new PassThroughProcess[size];
	private final List<ByteBuf> expectedData = new ArrayList<>();

	private Promise<Void> acknowledgement;
	private boolean consumedAll = false;

	@Before
	public void setUp() {
		Random random = new Random();
		for (int i = 0; i < 5; i++) {
			byte[] bytes = new byte[1000];
			random.nextBytes(bytes);
			ByteBuf buf = ByteBufPool.allocate(bytes.length);
			buf.put(bytes);
			expectedData.add(buf);
		}
		for (int i = 0; i < size; i++) {
			processes[i] = new PassThroughProcess();
		}

		for (int i = 0; i < size - 1; i++) {
			processes[i].getOutput().bindTo(processes[i + 1].getInput());
		}

		acknowledgement = ChannelSupplier.ofList(expectedData)
				.bindTo(processes[0].getInput());
	}

	@Test
	public void testAckPropagation() {
		processes[size - 1].getOutput()
				.set(ChannelConsumer.of(value -> {
					actualData.add(value);
					if (expectedData.size() == actualData.size()) {
						Recyclers.recycle(actualData);
						consumedAll = true;
					}
					return Promise.complete();
				}));

		await(acknowledgement);
		assertTrue(consumedAll);
	}

	@Test
	public void testAckPropagationWithFailure() {
		processes[size - 1].getOutput()
				.set(ChannelConsumer.of(value -> {
					Recyclers.recycle(value);
					return Promise.ofException(ERROR);
				}));

		assertSame(ERROR, awaitException(acknowledgement));
	}

	// region stub
	static class PassThroughProcess extends AbstractCommunicatingProcess implements WithChannelTransformer<PassThroughProcess, ByteBuf, ByteBuf> {
		ChannelSupplier<ByteBuf> input;
		ChannelConsumer<ByteBuf> output;

		@Override
		public ChannelInput<ByteBuf> getInput() {
			return input -> {
				this.input = input;
				if (this.input != null && this.output != null) startProcess();
				return getProcessCompletion();
			};
		}

		@Override
		public ChannelOutput<ByteBuf> getOutput() {
			return output -> {
				this.output = output;
				if (this.input != null && this.output != null) startProcess();
			};
		}

		@Override
		protected void doProcess() {
			input.get()
					.whenComplete((data, e) -> {
						if (data == null) {
							output.acceptEndOfStream()
									.whenResult(this::completeProcess)
									.whenException(this::closeEx);
						} else {
							output.accept(data)
									.whenResult(this::doProcess)
									.whenException(this::closeEx);
						}
					});
		}

		@Override
		protected void doClose(Throwable e) {
			output.closeEx(e);
			input.closeEx(e);
		}
	}
	// endregion
}
