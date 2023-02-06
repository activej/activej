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

package io.activej.datastream.supplier;

import io.activej.common.Checks;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.ImplicitlyReactive;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * This is a helper partial implementation of the {@link StreamSupplier}
 * which helps to deal with state transitions and helps to implement basic behaviours.
 */
public abstract class AbstractStreamSupplier<T> extends ImplicitlyReactive implements StreamSupplier<T> {
	private static final boolean CHECKS = Checks.isEnabled(AbstractStreamSupplier.class);

	public static final StreamDataAcceptor<?> NO_ACCEPTOR = item -> {};

	private @Nullable StreamDataAcceptor<T> dataAcceptor;
	private StreamDataAcceptor<T> dataAcceptorBuffered;
	private final ArrayDeque<T> buffer = new ArrayDeque<>();

	private StreamConsumer<T> consumer;

	private boolean flushRequest;
	private boolean flushRunning;
	private boolean initialized;
	private int flushAsync;

	private boolean endOfStreamRequest;
	private final SettablePromise<Void> endOfStream = new SettablePromise<>();
	private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

	private @Nullable SettablePromise<Void> flushPromise;

	{
		dataAcceptorBuffered = buffer::addLast;
		if (reactor.inReactorThread()) {
			reactor.post(this::ensureInitialized);
		} else {
			reactor.execute(this::ensureInitialized);
		}
	}

	@Override
	public final Promise<Void> streamTo(StreamConsumer<T> consumer) {
		checkInReactorThread(this);
		checkState(!isStarted());
		ensureInitialized();
		this.consumer = consumer;
		consumer.getAcknowledgement()
				.whenResult(this::acknowledge)
				.whenException(this::closeEx);
		if (!isEndOfStream()) {
			onStarted();
		}
		//noinspection unchecked
		this.dataAcceptor = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		consumer.consume(this);
		updateDataAcceptor();
		return acknowledgement;
	}

	/**
	 * This method will be called exactly once: either in the next reactor tick after creation of this supplier
	 * or right before {@link #onStarted()} or {@link #onError(Exception)} calls
	 */
	protected void onInit() {
	}

	protected void onStarted() {
	}

	public final boolean isStarted() {
		return consumer != null;
	}

	public final StreamConsumer<T> getConsumer() {
		return consumer;
	}

	@Override
	public final void updateDataAcceptor() {
		checkInReactorThread(this);
		if (!isStarted()) return;
		if (endOfStream.isComplete()) return;
		StreamDataAcceptor<T> dataAcceptor = this.consumer.getDataAcceptor();
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		if (dataAcceptor != null) {
			if (!isEndOfStream()) {
				this.dataAcceptorBuffered = dataAcceptor;
			}
			flush();
		} else if (!isEndOfStream()) {
			this.dataAcceptorBuffered = buffer::addLast;
			onSuspended();
		}
	}

	protected final void asyncBegin() {
		flushAsync++;
	}

	protected final void asyncEnd() {
		checkState(flushAsync > 0);
		flushAsync--;
	}

	protected final void asyncResume() {
		checkState(flushAsync > 0);
		flushAsync--;
		resume();
	}

	protected final void resume() {
		if (flushRunning) {
			flushRequest = true;
		} else if (isReady() && !isEndOfStream()) {
			onResumed();
		}
	}

	/**
	 * Sends given item through this supplier.
	 * <p>
	 * This method stores the item to an internal buffer if supplier is in a suspended state,
	 * and must never be called when supplier reaches {@link #sendEndOfStream() end of stream}.
	 */
	public final void send(T item) {
		if (CHECKS) checkInReactorThread(this);
		dataAcceptorBuffered.accept(item);
	}

	/**
	 * Puts this supplier in closed state with no error.
	 * This operation is final and cannot be undone.
	 * Only the first call causes any effect.
	 */
	public final Promise<Void> sendEndOfStream() {
		checkInReactorThread(this);
		if (endOfStreamRequest) return flushPromise;
		if (flushAsync > 0) {
			asyncEnd();
		}
		endOfStreamRequest = true;
		//noinspection unchecked
		this.dataAcceptorBuffered = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		flush();
		return getFlushPromise();
	}

	/**
	 * Returns a promise that will be completed when all data items are propagated
	 * to the actual data acceptor
	 */
	public final Promise<Void> getFlushPromise() {
		if (isEndOfStream()) {
			return endOfStream;
		} else if (flushPromise != null) {
			return flushPromise;
		} else if (dataAcceptor != null) {
			return Promise.complete();
		} else {
			flushPromise = new SettablePromise<>();
			return flushPromise;
		}
	}

	/**
	 * Initializes this supplier by calling {@link #onInit()} only if it has not already been initialized.
	 */
	private void ensureInitialized() {
		if (!initialized) {
			initialized = true;
			onInit();
		}
	}

	/**
	 * Causes this supplier to try to supply its buffered items and updates the current state accordingly.
	 */
	private void flush() {
		flushRequest = true;
		if (flushRunning || flushAsync > 0) return; // recursive call
		if (endOfStream.isComplete()) return;
		if (!isStarted()) return;

		flushRunning = true;
		while (flushRequest) {
			flushRequest = false;
			while (isReady() && !buffer.isEmpty()) {
				T item = buffer.pollFirst();
				this.dataAcceptor.accept(item);
			}
			if (isReady() && !isEndOfStream()) {
				onResumed();
			}
		}
		flushRunning = false;

		if (flushAsync > 0) return;
		if (!buffer.isEmpty()) return;
		if (endOfStream.isComplete()) return;

		if (!endOfStreamRequest) {
			if (this.flushPromise != null) {
				SettablePromise<Void> flushPromise = this.flushPromise;
				this.flushPromise = null;
				flushPromise.set(null);
			}
			return;
		}

		dataAcceptor = null;
		if (flushPromise != null) {
			flushPromise.set(null);
		}
		endOfStream.set(null);
	}

	/**
	 * Called when this supplier changes from suspended state to a normal one.
	 */
	protected void onResumed() {
	}

	/**
	 * Called when this supplier changes a normal state to a suspended one.
	 */
	protected void onSuspended() {
	}

	/**
	 * Returns current data acceptor (the last one set with the {@link #updateDataAcceptor()} method)
	 * or <code>null</code> when this supplier is in a suspended state.
	 */
	public final @Nullable StreamDataAcceptor<T> getDataAcceptor() {
		return dataAcceptor;
	}

	public final StreamDataAcceptor<T> getBufferedDataAcceptor() {
		return dataAcceptorBuffered;
	}

	/**
	 * Returns <code>true</code> when this supplier is in normal state and
	 * <cod>false</cod> when it is suspended or closed.
	 */
	public final boolean isReady() {
		return dataAcceptor != null;
	}

	@Override
	public final Promise<Void> getEndOfStream() {
		return endOfStream;
	}

	public final boolean isEndOfStream() {
		return endOfStreamRequest;
	}

	private void acknowledge() {
		ensureInitialized();
		if (acknowledgement.trySet(null)) {
			onAcknowledge();
			close();
			cleanup();
		}
	}

	protected void onAcknowledge() {
	}

	@Override
	public final Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	@Override
	public final void closeEx(Exception e) {
		checkInReactorThread(this);
		ensureInitialized();
		endOfStreamRequest = true;
		dataAcceptor = null;
		//noinspection unchecked
		dataAcceptorBuffered = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		if (flushPromise != null) {
			flushPromise.trySetException(e);
		}
		endOfStream.trySetException(e);
		if (acknowledgement.trySetException(e)) {
			onError(e);
			cleanup();
		}
	}

	/**
	 * This method will be called when this supplier erroneously changes to the closed state.
	 */
	protected void onError(Exception e) {
	}

	private void cleanup() {
		onComplete();
		reactor.post(this::onCleanup);
		buffer.clear();
		if (flushPromise != null) {
			flushPromise.resetCallbacks();
		}
	}

	protected void onComplete() {
	}

	/**
	 * This method will be asynchronously called after this supplier changes to the closed state regardless of error.
	 */
	protected void onCleanup() {
	}
}
