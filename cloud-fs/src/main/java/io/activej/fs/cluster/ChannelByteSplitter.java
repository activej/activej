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
import io.activej.fs.exception.FileSystemException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.activej.common.Checks.checkState;
import static io.activej.fs.cluster.FileSystemPartitions.LOCAL_EXCEPTION;
import static io.activej.reactor.Reactive.checkInReactorThread;

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
			checkInReactorThread(this);
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
			checkInReactorThread(this);
			outputs.set(index, output);
			tryStart();
		};
	}

	private void tryStart() {
		if (input != null && outputs.stream().allMatch(Objects::nonNull)) {
			reactor.post(this::startProcess);
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
												.then(Promise::of,
														e -> {
															failed.add(output);
															if (outputs.size() - failed.size() < requiredSuccesses) {
																return Promise.ofException(e);
															}
															return Promise.complete();
														})))
								.whenComplete(() -> outputs.removeAll(failed))
								.whenException(e -> closeEx(new FileSystemException("Not enough successes")))
								.whenResult(this::doProcess);
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
