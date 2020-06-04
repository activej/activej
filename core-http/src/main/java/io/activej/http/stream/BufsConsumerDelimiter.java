/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.http.stream;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;

import static io.activej.common.Preconditions.checkState;

/**
 * This is a binary channel transformer, that converts channels of {@link ByteBuf ByteBufs}
 * limiting the number of bytes that is sent/received by its peer.
 */
public final class BufsConsumerDelimiter extends AbstractCommunicatingProcess
		implements WithChannelTransformer<BufsConsumerDelimiter, ByteBuf, ByteBuf>, WithBinaryChannelInput<BufsConsumerDelimiter> {

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	private int remaining;

	// region creators
	private BufsConsumerDelimiter(int remaining) {
		this.remaining = remaining;
	}

	public static BufsConsumerDelimiter create(int remaining) {
		checkState(remaining >= 0, "Cannot create delimiter with number of remaining bytes that is less than 0");
		return new BufsConsumerDelimiter(remaining);
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		if (remaining == 0) {
			input.endOfStream()
					.then(output::acceptEndOfStream)
					.whenResult(this::completeProcess);
			return;
		}
		ByteBufQueue outputBufs = new ByteBufQueue();
		remaining -= bufs.drainTo(outputBufs, remaining);
		output.acceptAll(outputBufs.asIterator())
				.whenResult(() -> {
					if (remaining != 0) {
						input.needMoreData()
								.whenResult(this::doProcess);
					} else {
						input.endOfStream()
								.then(output::acceptEndOfStream)
								.whenResult(this::completeProcess);
					}
				});
	}

	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		output.closeEx(e);
	}
}
