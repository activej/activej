package io.activej.datastream.consumer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.reactor.Reactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@ExposedInternals
public final class OfAnotherReactor<T> extends AbstractStreamConsumer<T> {
	public static final int MAX_BUFFER_SIZE = 100;
	public static final Iterator<?> END_OF_STREAM = Collections.emptyIterator();

	public List<T> list = new ArrayList<>();
	public final StreamDataAcceptor<T> toList = item -> {
		list.add(item);
		if (list.size() == MAX_BUFFER_SIZE) {
			flush();
			suspend();
		}
	};

	public final StreamConsumer<T> anotherReactorConsumer;
	public final InternalSupplier internalSupplier;
	public volatile boolean wakingUp;

	public OfAnotherReactor(Reactor anotherReactor, StreamConsumer<T> anotherReactorConsumer) {
		this.anotherReactorConsumer = anotherReactorConsumer;
		this.internalSupplier = Reactor.executeWithReactor(anotherReactor, InternalSupplier::new);
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
		if (internalSupplier.isReady) {
			resume(toList);
			internalSupplier.wakeUp();
		} else {
			suspend();
		}
	}

	@Override
	protected void onInit() {
		reactor.startExternalTask();
	}

	@Override
	protected void onStarted() {
		internalSupplier.execute(() ->
				internalSupplier.streamTo(anotherReactorConsumer));
	}

	@Override
	protected void onEndOfStream() {
		flush();
	}

	@Override
	protected void onComplete() {
		reactor.completeExternalTask();
	}

	private void flush() {
		if (internalSupplier.iterator != null) return;
		if (this.isEndOfStream() && this.list.isEmpty()) {
			//noinspection unchecked
			internalSupplier.iterator = (Iterator<T>) END_OF_STREAM;
		} else if (!this.list.isEmpty()) {
			internalSupplier.iterator = this.list.iterator();
			this.list = new ArrayList<>();
		} else {
			return;
		}
		internalSupplier.wakeUp();
	}

	@Override
	protected void onError(Exception e) {
		internalSupplier.execute(() -> internalSupplier.closeEx(e));
	}

	@Override
	protected void onCleanup() {
		this.list = null;
	}

	public final class InternalSupplier extends AbstractStreamSupplier<T> {
		volatile Iterator<T> iterator;
		volatile boolean isReady;
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
		}

		@Override
		protected void onInit() {
			reactor.startExternalTask();
		}

		@Override
		protected void onResumed() {
			isReady = true;
			flush();
		}

		@Override
		protected void onSuspended() {
			isReady = false;
			OfAnotherReactor.this.wakeUp();
		}

		@Override
		protected void onComplete() {
			reactor.completeExternalTask();
		}

		private void flush() {
			if (iterator == null) {
				OfAnotherReactor.this.wakeUp();
				return;
			}
			Iterator<T> iterator = this.iterator;
			while (isReady() && iterator.hasNext()) {
				send(iterator.next());
			}
			if (iterator == END_OF_STREAM) {
				sendEndOfStream();
			} else if (!this.iterator.hasNext()) {
				this.iterator = null;
				OfAnotherReactor.this.wakeUp();
			}
		}

		@Override
		protected void onAcknowledge() {
			OfAnotherReactor.this.execute(OfAnotherReactor.this::acknowledge);
		}

		@Override
		protected void onError(Exception e) {
			OfAnotherReactor.this.execute(() -> OfAnotherReactor.this.closeEx(e));
		}

		@Override
		protected void onCleanup() {
			this.iterator = null;
		}
	}
}
