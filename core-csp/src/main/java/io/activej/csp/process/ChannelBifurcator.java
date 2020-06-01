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

import io.activej.csp.AbstractCommunicatingProcess;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelInput;

import static io.activej.common.Preconditions.checkState;
import static io.activej.common.Recyclable.tryRecycle;
import static io.activej.common.Sliceable.trySlice;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.eventloop.RunnableWithContext.wrapContext;

/**
 * Communicating process which distributes
 * an input item to two output channels.
 *
 * @since 3.0.0
 */
//[START REGION_1]
public class ChannelBifurcator<T> extends AbstractCommunicatingProcess
		implements WithChannelInput<ChannelBifurcator<T>, T> {

	ChannelConsumer<T> first;
	ChannelConsumer<T> second;

	ChannelSupplier<T> input;

	private ChannelBifurcator() {
	}

	public static <T> ChannelBifurcator<T> create() {
		return new ChannelBifurcator<>();
	}

	public static <T> ChannelBifurcator<T> create(ChannelSupplier<T> supplier, ChannelConsumer<T> first, ChannelConsumer<T> second) {
		return new ChannelBifurcator<T>().withInput(supplier).withOutputs(first, second);
	}

	//[END REGION_1]

	/**
	 * Provides ability to create a Bifurcator in the following way:
	 * {@code  bifurcator = ChannelBifurcator.create().withInput(i).withOutputs(o1, o2); }
	 */
	//[START REGION_2]
	public ChannelBifurcator<T> withOutputs(ChannelConsumer<T> firstOutput, ChannelConsumer<T> secondOutput) {
		this.first = sanitize(firstOutput);
		this.second = sanitize(secondOutput);
		tryStart();
		return this;
	}
	//[END REGION_2]

	/**
	 * Bifurcator startup.
	 */
	//[START REGION_6]
	private void tryStart() {
		if (input != null && first != null && second != null) {
			getCurrentEventloop().post(wrapContext(this, this::startProcess));
		}
	}
	//[END REGION_6]

	/**
	 * Main process for our bifurcator.
	 * On every tick bifurcator (BF) checks if input item exists.
	 * If item exists, BF tries to send it to the output channels
	 * and continues listening to the input channel.
	 * <p>
	 * Note : if an item can be sliced into chunks,
	 * bifurcator will try to slice the input item
	 * before it is accepted by the output consumer.
	 */
	//[START REGION_4]
	@Override
	protected void doProcess() {
		if (isProcessComplete()) {
			return;
		}

		input.get()
				.whenComplete((item, e) -> {
					if (item != null) {
						first.accept(trySlice(item)).both(second.accept(trySlice(item)))
								.whenComplete(($, e1) -> {
									if (e1 == null) {
										doProcess();
									} else {
										closeEx(e1);
									}
								});
						tryRecycle(item);
					} else {
						first.acceptEndOfStream().both(second.acceptEndOfStream())
								.whenComplete(($, e2) -> completeProcessEx(e2));
					}
				});
	}
	//[END REGION_4]

	/**
	 * Closes all channels.
	 *
	 * @param e an exception thrown on closing
	 */
	//[START REGION_5]
	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		first.closeEx(e);
		second.closeEx(e);
	}
	//[END REGION_5]

	//[START REGION_3]
	@Override
	public ChannelInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure bifurcator while it is running");
			this.input = sanitize(input);
			tryStart();
			return getProcessCompletion();
		};
	}
	//[END REGION_3]
}

