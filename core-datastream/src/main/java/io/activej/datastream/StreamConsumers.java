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

import io.activej.common.function.ConsumerEx;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.Reactor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static io.activej.common.Utils.nullify;
import static io.activej.common.exception.FatalErrorHandlers.handleError;

public final class StreamConsumers {

	public static final class Idle<T> extends AbstractStreamConsumer<T> {
		@Override
		protected void onEndOfStream() {
			acknowledge();
		}
	}

	public static final class Skip<T> extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			resume(item -> {});
		}

		@Override
		protected void onEndOfStream() {
			acknowledge();
		}
	}

	public static final class OfConsumer<T> extends AbstractStreamConsumer<T> {
		private final ConsumerEx<T> consumer;

		OfConsumer(ConsumerEx<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			resume(item -> {
				try {
					consumer.accept(item);
				} catch (Exception ex) {
					handleError(ex, this);
					closeEx(ex);
				}
			});
		}

		@Override
		protected void onEndOfStream() {
			acknowledge();
		}
	}

	public static final class ClosingWithError<T> extends AbstractStreamConsumer<T> {
		private Exception error;

		ClosingWithError(Exception e) {
			this.error = e;
		}

		@Override
		protected void onInit() {
			error = nullify(error, this::closeEx);
		}
	}

	public static final class OfPromise<T> extends AbstractStreamConsumer<T> {
		private Promise<? extends StreamConsumer<T>> promise;
		private final InternalSupplier internalSupplier = new InternalSupplier();

		public class InternalSupplier extends AbstractStreamSupplier<T> {
			@Override
			protected void onResumed() {
				OfPromise.this.resume(getDataAcceptor());
			}

			@Override
			protected void onSuspended() {
				OfPromise.this.suspend();
			}
		}

		public OfPromise(Promise<? extends StreamConsumer<T>> promise) {
			this.promise = promise;
		}

		@Override
		protected void onInit() {
			promise
					.whenResult(consumer -> {
						consumer.getAcknowledgement()
								.whenResult(this::acknowledge)
								.whenException(this::closeEx);
						this.getAcknowledgement()
								.whenException(consumer::closeEx);
						internalSupplier.streamTo(consumer);
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onEndOfStream() {
			internalSupplier.sendEndOfStream();
		}

		@Override
		protected void onCleanup() {
			promise = null;
		}
	}

	public static final class OfChannelConsumer<T> extends AbstractStreamConsumer<T> {
		private final ChannelConsumer<T> consumer;
		private boolean working;

		OfChannelConsumer(ChannelConsumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			flush();
		}

		private void flush() {
			resume(item -> {
				Promise<Void> promise = consumer.accept(item)
						.whenException(this::closeEx);
				if (promise.isComplete()) return;
				suspend();
				working = true;
				promise.whenResult(() -> {
					working = false;
					if (!isEndOfStream()) {
						flush();
					} else {
						sendEndOfStream();
					}
				});
			});
		}

		@Override
		protected void onEndOfStream() {
			// end of stream is sent either from here or from queues waiting put promise
			// callback, but not from both and this condition ensures that
			if (!working) {
				sendEndOfStream();
			}
		}

		private void sendEndOfStream() {
			consumer.acceptEndOfStream()
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		}

		@Override
		protected void onError(Exception e) {
			consumer.closeEx(e);
		}
	}

	public static final class ToCollector<T, A, R> extends AbstractStreamConsumer<T> {
		private final SettablePromise<R> resultPromise = new SettablePromise<>();
		private final Collector<T, A, R> collector;
		private A accumulator;

		public ToCollector(Collector<T, A, R> collector) {
			this.collector = collector;
		}

		public Promise<R> getResult() {
			return resultPromise;
		}

		@Override
		protected void onInit() {
			resultPromise.whenResult(this::acknowledge);
		}

		@Override
		protected void onStarted() {
			A accumulator = collector.supplier().get();
			this.accumulator = accumulator;
			BiConsumer<A, T> consumer = collector.accumulator();
			resume(item -> consumer.accept(accumulator, item));
		}

		@Override
		protected void onEndOfStream() {
			resultPromise.set(collector.finisher().apply(accumulator));
		}

		@Override
		protected void onError(Exception e) {
			resultPromise.setException(e);
		}

		@Override
		protected void onCleanup() {
			accumulator = null;
		}

	}

	public static final class OfAnotherReactor<T> extends AbstractStreamConsumer<T> {
		private static final int MAX_BUFFER_SIZE = 100;
		private static final Iterator<?> END_OF_STREAM = Collections.emptyIterator();

		private List<T> list = new ArrayList<>();
		private final StreamDataAcceptor<T> toList = item -> {
			list.add(item);
			if (list.size() == MAX_BUFFER_SIZE) {
				flush();
				suspend();
			}
		};

		private final StreamConsumer<T> anotherReactorConsumer;
		private final InternalSupplier internalSupplier;
		private volatile boolean wakingUp;

		public OfAnotherReactor(Reactor anotherReactor, StreamConsumer<T> anotherReactorConsumer) {
			this.anotherReactorConsumer = anotherReactorConsumer;
			this.internalSupplier = Reactor.executeWithReactor(anotherReactor, InternalSupplier::new);
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

}
