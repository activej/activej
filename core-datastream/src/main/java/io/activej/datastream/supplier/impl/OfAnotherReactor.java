package io.activej.datastream.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.Reactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@ExposedInternals
public final class OfAnotherReactor<T> extends AbstractStreamSupplier<T> {
	public static final int MAX_BUFFER_SIZE = 100;
	public static final Iterator<?> END_OF_STREAM = Collections.emptyIterator();

	public volatile Iterator<T> iterator;
	public volatile boolean isReady;

	public final StreamSupplier<T> anotherReactorSupplier;
	public final InternalConsumer internalConsumer;
	public volatile boolean wakingUp;

	public OfAnotherReactor(Reactor anotherReactor, StreamSupplier<T> anotherReactorSupplier) {
		this.anotherReactorSupplier = anotherReactorSupplier;
		this.internalConsumer = Reactor.executeWithReactor(anotherReactor, InternalConsumer::new);
	}

	private void execute(Runnable runnable) {
		reactor.execute(runnable);
	}

	private void wakeUp() {
		if (wakingUp) return;
		wakingUp = true;
		execute(this::onWakeUp);
	}

	private void onWakeUp() {
		if (isComplete()) return;
		wakingUp = false;
		flush();
	}

	@Override
	protected void onInit() {
		reactor.startExternalTask();
	}

	@Override
	protected void onStarted() {
		internalConsumer.execute(() ->
				anotherReactorSupplier.streamTo(internalConsumer));
	}

	@Override
	protected void onResumed() {
		isReady = true;
		flush();
	}

	@Override
	protected void onSuspended() {
		isReady = false;
		internalConsumer.wakeUp();
	}

	@Override
	protected void onComplete() {
		reactor.completeExternalTask();
	}

	private void flush() {
		if (iterator == null) {
			internalConsumer.wakeUp();
			return;
		}
		Iterator<T> iterator = this.iterator;
		while (isReady() && iterator.hasNext()) {
			send(iterator.next());
		}
		if (iterator == END_OF_STREAM) {
			sendEndOfStream();
		} else if (!iterator.hasNext()) {
			this.iterator = null;
			internalConsumer.wakeUp();
		}
	}

	@Override
	protected void onAcknowledge() {
		internalConsumer.execute(internalConsumer::acknowledge);
	}

	@Override
	protected void onError(Exception e) {
		internalConsumer.execute(() -> internalConsumer.closeEx(e));
	}

	@Override
	protected void onCleanup() {
		this.iterator = null;
	}

	public final class InternalConsumer extends AbstractStreamConsumer<T> {
		private List<T> list = new ArrayList<>();
		private final StreamDataAcceptor<T> toList = item -> {
			list.add(item);
			if (list.size() == MAX_BUFFER_SIZE) {
				flush();
				suspend();
			}
		};
		volatile boolean wakingUp;

		void execute(Runnable runnable) {
			reactor.execute(runnable);
		}

		void wakeUp() {
			if (wakingUp) return;
			wakingUp = true;
			execute(this::onWakeUp);
		}

		void onWakeUp() {
			if (isComplete()) return;
			wakingUp = false;
			flush();
			if (OfAnotherReactor.this.isReady) {
				resume(toList);
			} else {
				suspend();
			}
		}

		@Override
		protected void onInit() {
			reactor.startExternalTask();
		}

		@Override
		protected void onEndOfStream() {
			flush();
		}

		private void flush() {
			if (OfAnotherReactor.this.iterator != null) return;
			if (this.isEndOfStream() && this.list.isEmpty()) {
				//noinspection unchecked
				OfAnotherReactor.this.iterator = (Iterator<T>) END_OF_STREAM;
			} else if (!this.list.isEmpty()) {
				OfAnotherReactor.this.iterator = this.list.iterator();
				this.list = new ArrayList<>();
			} else {
				return;
			}
			OfAnotherReactor.this.wakeUp();
		}

		@Override
		protected void onError(Exception e) {
			OfAnotherReactor.this.execute(() -> OfAnotherReactor.this.closeEx(e));
		}

		@Override
		protected void onComplete() {
			reactor.completeExternalTask();
		}

		@Override
		protected void onCleanup() {
			this.list = null;
		}
	}

}
