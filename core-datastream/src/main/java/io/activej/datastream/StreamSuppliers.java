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

import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.activej.common.Utils.nullify;

final class StreamSuppliers {

	static final class ClosingWithError<T> extends AbstractStreamSupplier<T> {
		private Exception error;

		ClosingWithError(Exception e) {
			this.error = e;
		}

		@Override
		protected void onInit() {
			error = nullify(error, this::closeEx);
		}
	}

	static final class Closing<T> extends AbstractStreamSupplier<T> {
		@Override
		protected void onInit() {
			sendEndOfStream();
		}
	}

	static final class Idle<T> extends AbstractStreamSupplier<T> {
	}

	static final class OfIterator<T> extends AbstractStreamSupplier<T> {
		private final Iterator<T> iterator;

		/**
		 * Creates a new instance of  StreamSupplierOfIterator
		 *
		 * @param iterator iterator with object which need to send
		 */
		public OfIterator(Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected void onResumed() {
			while (isReady() && iterator.hasNext()) {
				send(iterator.next());
			}
			if (!iterator.hasNext()) {
				sendEndOfStream();
			}
		}
	}

	static final class OfPromise<T> extends AbstractStreamSupplier<T> {
		private Promise<? extends StreamSupplier<T>> promise;
		private final InternalConsumer internalConsumer = new InternalConsumer();

		private class InternalConsumer extends AbstractStreamConsumer<T> {}

		public OfPromise(Promise<? extends StreamSupplier<T>> promise) {
			this.promise = promise;
		}

		@Override
		protected void onInit() {
			promise
					.whenResult(supplier -> {
						this.getEndOfStream()
								.whenException(supplier::closeEx);
						supplier.getEndOfStream()
								.whenResult(this::sendEndOfStream)
								.whenException(this::closeEx);
						supplier.streamTo(internalConsumer);
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onResumed() {
			internalConsumer.resume(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			internalConsumer.suspend();
		}

		@Override
		protected void onAcknowledge() {
			internalConsumer.acknowledge();
		}

		@Override
		protected void onCleanup() {
			promise = null;
		}
	}

	static final class Concat<T> extends AbstractStreamSupplier<T> {
		private ChannelSupplier<StreamSupplier<T>> iterator;
		private InternalConsumer internalConsumer = new InternalConsumer();

		private class InternalConsumer extends AbstractStreamConsumer<T> {}

		Concat(ChannelSupplier<StreamSupplier<T>> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected void onStarted() {
			next();
		}

		private void next() {
			internalConsumer.acknowledge();
			internalConsumer = new InternalConsumer();
			resume();
			iterator.get()
					.whenResult(supplier -> {
						if (supplier != null) {
							supplier.getEndOfStream()
									.whenResult(this::next)
									.whenException(this::closeEx);
							supplier.streamTo(internalConsumer);
						} else {
							sendEndOfStream();
						}
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onResumed() {
			internalConsumer.resume(getDataAcceptor());
		}

		@Override
		protected void onAcknowledge() {
			internalConsumer.acknowledge();
		}

		@Override
		protected void onSuspended() {
			internalConsumer.suspend();
		}

		@Override
		protected void onError(Exception e) {
			internalConsumer.closeEx(e);
			iterator.closeEx(e);
		}

		@Override
		protected void onCleanup() {
			iterator = null;
		}
	}

	static final class OfChannelSupplier<T> extends AbstractStreamSupplier<T> {
		private final ChannelSupplier<T> supplier;

		public OfChannelSupplier(ChannelSupplier<T> supplier) {
			this.supplier = supplier;
		}

		@Override
		protected void onResumed() {
			asyncBegin();
			supplier.get()
					.run((item, e) -> {
						if (e == null) {
							if (item != null) {
								send(item);
								asyncResume();
							} else {
								sendEndOfStream();
							}
						} else {
							closeEx(e);
						}
					});
		}

		@Override
		protected void onError(Exception e) {
			supplier.closeEx(e);
		}
	}

	static final class OfAnotherReactor<T> extends AbstractStreamSupplier<T> {
		private static final int MAX_BUFFER_SIZE = 100;
		private static final Iterator<?> END_OF_STREAM = Collections.emptyIterator();

		private volatile Iterator<T> iterator;
		private volatile boolean isReady;

		private final StreamSupplier<T> anotherReactorSupplier;
		private final InternalConsumer internalConsumer;
		private volatile boolean wakingUp;

		public OfAnotherReactor(Reactor anotherReactor, StreamSupplier<T> anotherReactorSupplier) {
			this.anotherReactorSupplier = anotherReactorSupplier;
			this.internalConsumer = Reactor.executeWithReactor(anotherReactor, InternalConsumer::new);
		}

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

		final class InternalConsumer extends AbstractStreamConsumer<T> {
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
}
