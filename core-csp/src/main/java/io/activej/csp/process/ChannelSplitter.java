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

package io.activej.csp.process;

import io.activej.common.recycle.Recyclers;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelInput;
import io.activej.csp.dsl.WithChannelOutputs;
import io.activej.promise.Promises;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static io.activej.common.Checks.checkState;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;

public final class ChannelSplitter<T> extends AbstractCommunicatingProcess
		implements WithChannelInput<ChannelSplitter<T>, T>, WithChannelOutputs<T> {
	private final List<ChannelConsumer<T>> outputs = new ArrayList<>();

	private ChannelSupplier<T> input;
	private Function<T, T> splitFn = Function.identity();

	private ChannelSplitter() {
	}

	public static <T> ChannelSplitter<T> create() {
		return new ChannelSplitter<>();
	}

	public static <T> ChannelSplitter<T> create(ChannelSupplier<T> input) {
		return new ChannelSplitter<T>().withInput(input);
	}

	/**
	 * Allows to set a function that will be used for splitting a value.
	 * A split function is applied to incoming value each time it is being sent to an output.
	 *
	 * @param splitFn split function
	 * @return this {@link ChannelSplitter}
	 */
	public ChannelSplitter<T> withSplitFunction(Function<T, T> splitFn) {
		this.splitFn = splitFn;
		return this;
	}

	public boolean hasOutputs() {
		return !outputs.isEmpty();
	}

	@Override
	public ChannelInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure splitter while it is running");
			this.input = sanitize(input);
			tryStart();
			return getProcessCompletion();
		};
	}

	@Override
	public ChannelOutput<T> addOutput() {
		int index = outputs.size();
		outputs.add(null);
		return output -> {
			outputs.set(index, sanitize(output));
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
		input.get()
				.run((item, e) -> {
					if (e == null) {
						if (item != null) {
							Promises.all(outputs.stream().map(output -> output.accept(splitFn.apply(item))))
									.run(($, e2) -> {
										if (e2 == null) {
											doProcess();
										} else {
											closeEx(e2);
										}
									});
							Recyclers.recycle(item);
						} else {
							Promises.all(outputs.stream().map(ChannelConsumer::acceptEndOfStream))
									.run(this::closeEx);
						}
					} else {
						closeEx(e);
					}
				});
	}

	@Override
	protected void doClose(Exception e) {
		input.closeEx(e);
		outputs.forEach(output -> output.closeEx(e));
	}
}
