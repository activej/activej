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

package io.activej.csp.process.transformer;

import io.activej.common.Checks;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

public abstract class AbstractChannelTransformer<S extends AbstractChannelTransformer<S, I, O>, I, O>
	extends AbstractCommunicatingProcess
	implements WithChannelTransformer<S, I, O> {

	private static final boolean CHECKS = Checks.isEnabled(AbstractChannelTransformer.class);

	protected ChannelSupplier<I> input;
	protected ChannelConsumer<O> output;

	protected final Promise<Void> send(O item) {
		return output.accept(item);
	}

	protected final Promise<Void> sendEndOfStream() {
		return output.acceptEndOfStream();
	}

	protected abstract Promise<Void> onItem(I item);

	protected Promise<Void> onProcessFinish() {
		return sendEndOfStream();
	}

	protected Promise<Void> onProcessStart() {
		return Promise.complete();
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		Promise.complete()
			.then(this::onProcessStart)
			.then(() -> input.streamTo(ChannelConsumers.ofAsyncConsumer(this::onItem)))
			.then(this::onProcessFinish)
			.whenResult(this::completeProcess)
			.whenException(this::closeEx);
	}

	@SuppressWarnings("ConstantConditions")
	@Override
	public ChannelInput<I> getInput() {
		return input -> {
			if (CHECKS) checkInReactorThread(this);
			this.input = sanitize(input);
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions")
	@Override
	public ChannelOutput<O> getOutput() {
		return output -> {
			if (CHECKS) checkInReactorThread(this);
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	protected final void doClose(Exception e) {
		reactor.post(this::onCleanup);
		input.closeEx(e);
		output.closeEx(e);
	}

	protected void onCleanup() {
	}
}
