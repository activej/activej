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

package io.activej.fs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelInputs;
import io.activej.csp.dsl.WithChannelOutput;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.activej.common.Checks.checkState;
import static io.activej.fs.cluster.FileSystemPartitions.LOCAL_EXCEPTION;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class ChannelByteCombiner extends AbstractCommunicatingProcess
		implements WithChannelInputs<ByteBuf>, WithChannelOutput<ChannelByteCombiner, ByteBuf> {

	private final List<ChannelSupplier<ByteBuf>> inputs = new ArrayList<>();
	private ChannelConsumer<ByteBuf> output;

	private long outputOffset;
	private long errorCount;

	private ChannelByteCombiner() {}

	public static ChannelByteCombiner create() {
		return new ChannelByteCombiner();
	}

	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkInReactorThread(this);
			checkState(!isProcessStarted());
			this.output = sanitize(output);
			tryStart();
		};
	}

	private void tryStart() {
		if (output != null && inputs.stream().allMatch(Objects::nonNull)) {
			reactor.post(this::startProcess);
		}
	}

	@Override
	public ChannelInput<ByteBuf> addInput() {
		int index = inputs.size();
		inputs.add(null);
		return input -> {
			checkInReactorThread(this);
			inputs.set(index, input);
			return getProcessCompletion();
		};
	}

	@Override
	protected void doProcess() {
		Promises.all(inputs.stream().map(this::doProcessInput))
				.whenException(output::closeEx)
				.then($ -> output.acceptEndOfStream())
				.whenComplete(this::completeProcess);
	}

	private Promise<Void> doProcessInput(ChannelSupplier<ByteBuf> input) {
		RefLong inputOffset = new RefLong(0);
		return Promises.repeat(
				() -> input.get()
						.then(Promise::of,
								e -> ++errorCount == inputs.size() ?
										Promise.ofException(e) :
										Promise.of(null))
						.thenIfElse(Objects::isNull,
								$ -> Promise.of(false),
								buf -> {
									int toSkip = (int) Math.min(outputOffset - inputOffset.value, buf.readRemaining());
									inputOffset.value += buf.readRemaining();
									buf.moveHead(toSkip);
									if (!buf.canRead()) {
										buf.recycle();
										return Promise.of(true);
									}
									outputOffset += buf.readRemaining();
									return output.accept(buf).map($ -> true);
								}));
	}

	@Override
	protected void doClose(Exception e) {
		// not passing the exception to all the outputs,
		// so that they wouldn't be marked dead
		inputs.forEach(input -> input.closeEx(LOCAL_EXCEPTION));

		output.closeEx(e);
	}
}
