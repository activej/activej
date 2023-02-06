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

package io.activej.datastream.processor.transformer.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.processor.transformer.StreamTransformer;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.reactor.ImplicitlyReactive;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nullify;

/**
 * Provides you apply function before sending data to the destination. It is a {@link RateLimiter}
 * which receives specified type and streams set of function's result  to the destination .
 */
@ExposedInternals
public final class RateLimiter<T> extends ImplicitlyReactive implements StreamTransformer<T, T> {
	private static final Duration MILLIS_DURATION = ChronoUnit.MILLIS.getDuration();

	public final double refillRatePerMillis;

	public double tokens;
	public long lastRefillTimestamp;
	public Tokenizer<T> tokenizer = $ -> 1;

	public final Input input;
	public final Output output;

	public @Nullable ScheduledRunnable scheduledRunnable;

	public RateLimiter(double refillRatePerMillis) {
		this.refillRatePerMillis = refillRatePerMillis;
		this.input = new Input();
		this.output = new Output();

		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getAcknowledgement()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
	}

	public static <T> RateLimiter<T> create(double refillRate, ChronoUnit perUnit) {
		return RateLimiter.<T>builder(refillRate, perUnit).build();
	}

	public static <T> RateLimiter<T>.Builder builder(double refillRate, ChronoUnit perUnit) {
		checkArgument(refillRate >= 0, "Negative refill rate");

		Duration perUnitDuration = perUnit.getDuration();
		double refillRatePerMillis;
		if (perUnit.ordinal() > ChronoUnit.MILLIS.ordinal()) {
			refillRatePerMillis = refillRate / perUnitDuration.dividedBy(MILLIS_DURATION);
		} else {
			refillRatePerMillis = refillRate * MILLIS_DURATION.dividedBy(perUnitDuration);
		}
		return new RateLimiter<T>(refillRatePerMillis).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RateLimiter<T>> {
		private Builder() {}

		public Builder withInitialTokens(long initialTokens) {
			checkNotBuilt(this);
			RateLimiter.this.tokens = initialTokens;
			return this;
		}

		public Builder withTokenizer(Tokenizer<T> tokenizer) {
			checkNotBuilt(this);
			RateLimiter.this.tokenizer = tokenizer;
			return this;
		}

		@Override
		protected RateLimiter<T> doBuild() {
			return RateLimiter.this;
		}
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	public final class Input extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
		@Override
		protected void onStarted() {
			lastRefillTimestamp = reactor.currentTimeMillis();
			resume(this);
		}

		@Override
		protected void onEndOfStream() {
			if (scheduledRunnable == null || scheduledRunnable.isComplete()) {
				output.sendEndOfStream();
			}
		}

		@Override
		protected void onError(Exception e) {
			scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
		}

		@Override
		public void accept(T item) {
			long itemTokens = tokenizer.getTokens(item);
			tokens -= itemTokens;

			if (tokens >= 0) {
				output.send(item);
				return;
			}

			input.suspend();
			output.send(item);

			if (scheduledRunnable != null) {
				return;
			}

			scheduledRunnable = RateLimiter.this.reactor.delay(
					calculateDelay(itemTokens),
					() -> output.proceed(itemTokens)
			);
		}
	}

	public final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			if (input.isEndOfStream()) {
				output.sendEndOfStream();
			} else {
				input.resume(input);
			}
		}

		@Override
		protected void onSuspended() {
			input.suspend();
		}

		public void proceed(long itemTokens) {
			scheduledRunnable = null;

			refill();
			if (tokens >= itemTokens) {
				resume();
				return;
			}

			scheduledRunnable = reactor.delay(calculateDelay(itemTokens), () -> proceed(itemTokens));
		}

		private void refill() {
			long timestamp = reactor.currentTimeMillis();
			double passedMillis = timestamp - lastRefillTimestamp;

			tokens += passedMillis * refillRatePerMillis;
			lastRefillTimestamp = timestamp;
		}
	}

	private long calculateDelay(long itemTokens) {
		double missing = itemTokens - tokens;
		assert missing > 0;

		return (long) Math.ceil(missing / refillRatePerMillis);
	}

	public interface Tokenizer<T> {
		long getTokens(T item);

		static Tokenizer<ByteBuf> forByteBufs() {
			return ByteBuf::readRemaining;
		}
	}
}
