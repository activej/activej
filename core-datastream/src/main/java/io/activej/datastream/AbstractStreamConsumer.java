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

package io.activej.datastream;

import io.activej.common.Check;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Preconditions.checkState;

/**
 * This is a helper partial implementation of the {@link StreamConsumer}
 * which helps to deal with state transitions and helps to implement basic behaviours.
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	private final boolean CHECK = Check.isEnabled(getClass());

	private StreamSupplier<T> supplier;
	private final SettablePromise<Void> acknowledgement = new SettablePromise<>();
	private boolean endOfStream;
	private boolean initialized;
	private @Nullable StreamDataAcceptor<T> dataAcceptor;

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();

	{
		if (eventloop.inEventloopThread()){
			eventloop.post(this::tryInitialize);
		} else {
			eventloop.execute(this::tryInitialize);
		}
	}

	@Override
	public final void consume(@NotNull StreamSupplier<T> streamSupplier) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		checkState(!isStarted());
		tryInitialize();
		if (acknowledgement.isComplete()) return;
		this.supplier = streamSupplier;
		if (!streamSupplier.isException()) {
			onStarted();
		}
		streamSupplier.getEndOfStream()
				.whenResult(this::endOfStream)
				.whenException(this::closeEx);
	}

	@Nullable
	@Override
	public final StreamDataAcceptor<T> getDataAcceptor() {
		return dataAcceptor;
	}

	/**
	 * 	 This method will be called exactly once: either in the next eventloop tick after creation of this supplier
	 * 	 or right before {@link #onStarted()} or {@link #onError(Throwable)} calls
	 */
	protected void onInit() {
	}

	/**
	 * This method will be called when this consumer begins receiving items.
	 * It may not be called if consumer never received anything getting closed.
	 */
	protected void onStarted() {
	}

	public final boolean isStarted() {
		return this.supplier != null;
	}

	public final StreamSupplier<T> getSupplier() {
		return supplier;
	}

	private void endOfStream() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (endOfStream) return;
		endOfStream = true;
		onEndOfStream();
	}

	/**
	 * This method will be called when associated supplier closes.
	 */
	protected void onEndOfStream() {
	}

	/**
	 * Begins receiving data into given acceptor, resumes the associated supplier to receive data from it.
	 */
	public final void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (endOfStream) return;
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		if (!isStarted()) return;
		supplier.updateDataAcceptor();
	}

	/**
	 * Suspends the associated supplier.
	 */
	public final void suspend() {
		resume(null);
	}

	/**
	 * Triggers the {@link #getAcknowledgement() acknowledgement} of this consumer.
	 */
	public final void acknowledge() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		endOfStream = true;
		if (acknowledgement.trySet(null)) {
			tryInitialize();
			cleanup();
		}
	}

	@Override
	public final Promise<Void> getAcknowledgement() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		return acknowledgement;
	}

	public final boolean isEndOfStream() {
		return endOfStream;
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		endOfStream = true;
		if (acknowledgement.trySetException(e)) {
			tryInitialize();
			onError(e);
			cleanup();
		}
	}

	/**
	 * This method will be called when this consumer erroneously changes to the acknowledged state.
	 */
	protected void onError(Throwable e) {
	}

	/**
	 * Initializes this consumer by calling {@link #onInit()} only if it has not already been initialized.
	 */
	private void tryInitialize() {
		if (!initialized) {
			initialized = true;
			onInit();
		}
	}

	private void cleanup() {
		onComplete();
		eventloop.post(this::onCleanup);
		acknowledgement.resetCallbacks();
		dataAcceptor = null;
	}

	protected void onComplete() {
	}

	/**
	 * This method will be asynchronously called after this consumer changes to the acknowledged state regardless of error.
	 */
	protected void onCleanup() {
	}
}
