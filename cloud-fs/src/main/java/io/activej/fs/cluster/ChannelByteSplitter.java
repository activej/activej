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
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelInput;
import io.activej.csp.dsl.WithChannelOutputs;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.fs.exception.FsException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.activej.common.Checks.checkState;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.fs.cluster.FsPartitions.LOCAL_EXCEPTION;

final class ChannelByteSplitter extends AbstractCommunicatingProcess
		implements WithChannelInput<ChannelByteSplitter, ByteBuf>, WithChannelOutputs<ByteBuf> {
	private final List<ChannelConsumer<ByteBuf>> outputs = new ArrayList<>();
	private final int requiredSuccesses;

	private ChannelSupplier<ByteBuf> input;

	private ChannelByteSplitter(int requiredSuccesses) {
		this.requiredSuccesses = requiredSuccesses;
	}

	public static ChannelByteSplitter create(int requiredSuccesses) {
		return new ChannelByteSplitter(requiredSuccesses);
	}

	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure splitter while it is running");
			this.input = sanitize(input);
			tryStart();
			return getProcessCompletion();
		};
	}

	@Override
	public ChannelOutput<ByteBuf> addOutput() {
		int index = outputs.size();
		outputs.add(null);
		return output -> {
			outputs.set(index, output);
			tryStart();
		};
	}

	private void tryStart() {
		if (input != null && outputs.stream().allMatch(Objects::nonNull)) {
			getCurrentEventloop().post(this::startProcess);
		}
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "No splitter input");
		checkState(!outputs.isEmpty(), "No splitter outputs");
	}

	@Override
	protected void doProcess() {
		if (isProcessComplete()) {
			return;
		}
		List<ChannelConsumer<ByteBuf>> failed = new ArrayList<>();
		input.get()
				.whenResult(buf -> {
					if (buf != null) {
						Promises.all(outputs.stream()
										.map(output -> output.accept(buf.slice())
												.then(Promise::of, e1 -> {
													failed.add(output);
													if (outputs.size() - failed.size() < requiredSuccesses) {
														return Promise.ofException(e1);
													}
													return Promise.complete();
												})))
								.whenComplete(($, e) -> {
									outputs.removeAll(failed);
									if (e == null) {
										doProcess();
									} else {
										closeEx(new FsException("Not enough successes"));
									}
								});
						buf.recycle();
					} else {
						Promises.all(outputs.stream().map(ChannelConsumer::acceptEndOfStream))
								.whenComplete(($, e) -> completeProcessEx(e));
					}
				})
				.whenException(this::closeEx);
	}

	@Override
	protected void doClose(Exception e) {
		input.closeEx(e);

		// not passing the exception to all the outputs,
		// so that they wouldn't be marked dead
		outputs.forEach(output -> output.closeEx(LOCAL_EXCEPTION));
	}
}
