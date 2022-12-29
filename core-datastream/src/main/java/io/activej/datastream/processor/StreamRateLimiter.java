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

package io.activej.datastream.processor;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.process.ChannelRateLimiter;
import io.activej.datastream.*;
import io.activej.reactor.ImplicitlyReactive;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Queue;

import static io.activej.common.Utils.nullify;

/**
 * Provides you apply function before sending data to the destination. It is a {@link StreamRateLimiter}
 * which receives specified type and streams set of function's result  to the destination .
 */
public final class StreamRateLimiter<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	private final long refillRatePerSecond;

	private long tokens;
	private long lastRefillTimestamp;
	private Tokenizer<T> tokenizer = $ -> 1;

	private final Input input;
	private final Output output;

	private @Nullable ScheduledRunnable scheduledRunnable;

	private StreamRateLimiter(long refillRatePerSecond) {
		this.refillRatePerSecond = refillRatePerSecond;
		this.input = new Input();
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> StreamRateLimiter<T> create(long refillRatePerSecond) {
		return new StreamRateLimiter<>(refillRatePerSecond);
	}

	public StreamRateLimiter<T> withInitialTokens(long initialTokens) {
		this.tokens = initialTokens;
		return this;
	}

	public StreamRateLimiter<T> withTokenizer(Tokenizer<T> tokenizer) {
		this.tokenizer = tokenizer;
		return this;
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	private final class Input extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		private final Queue<T> buffer = new ArrayDeque<>();

		@Override
		protected void onStarted() {
			lastRefillTimestamp = reactor.currentTimeMillis();
			resume(this);
		}

		@Override
		protected void onEndOfStream() {
			if (scheduledRunnable == null || scheduledRunnable.isComplete()){
				output.sendEndOfStream();
			}
		}

		@Override
		protected void onError(Exception e) {
			scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
		}

		@Override
		public void accept(T item) {
			if (!buffer.isEmpty()) {
				buffer.add(item);
				return;
			}

			long itemTokens = tokenizer.getTokens(item);
			if (itemTokens <= tokens) {
				tokens -= itemTokens;
				output.send(item);
				return;
			}

			buffer.add(item);
			input.suspend();

			scheduledRunnable = StreamRateLimiter.this.reactor.delay(calculateDelay(itemTokens), output::proceed);
		}

		public boolean flush() {
			while (!buffer.isEmpty() && output.isReady()) {
				long itemTokens = tokenizer.getTokens(buffer.peek());
				if (itemTokens > tokens) return false;
				tokens -= itemTokens;
				output.send(buffer.poll());
			}
			return buffer.isEmpty();
		}
	}

	private final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			refill();
			if (input.flush()) {
				if (input.isEndOfStream()) {
					output.sendEndOfStream();
				} else {
					input.resume(input);
				}
			} else {
				if (!input.buffer.isEmpty()) {
					long totalDelay = input.buffer.stream()
							.map(tokenizer::getTokens)
							.mapToLong(StreamRateLimiter.this::calculateDelay)
							.sum();

					scheduledRunnable = reactor.delay(totalDelay, output::proceed);
				}
				input.suspend();
			}
		}

		@Override
		protected void onSuspended() {
			input.suspend();
		}

		public void proceed() {
			scheduledRunnable = null;
			resume();
		}
	}

	private void refill() {
		long timestamp = reactor.currentTimeMillis();
		long passedMillis = timestamp - lastRefillTimestamp;

		tokens += (long) (passedMillis / 1000.0d * refillRatePerSecond);
		lastRefillTimestamp = timestamp;
	}

	private long calculateDelay(long itemTokens) {
		long missing = itemTokens - tokens;
		assert missing > 0;

		double secondsToRefill = (double) missing / refillRatePerSecond;

		return Math.max(1, (long) (secondsToRefill * 1000));
	}

	public interface Tokenizer<T> {
		long getTokens(T item);

		static ChannelRateLimiter.Tokenizer<ByteBuf> forByteBufs() {
			return ByteBuf::readRemaining;
		}
	}
}
