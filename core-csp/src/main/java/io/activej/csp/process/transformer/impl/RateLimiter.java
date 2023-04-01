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

package io.activej.csp.process.transformer.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.process.transformer.AbstractChannelTransformer;
import io.activej.promise.Promise;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nullify;

@ExposedInternals
public final class RateLimiter<T> extends AbstractChannelTransformer<RateLimiter<T>, T, T> {
	private static final Duration MILLIS_DURATION = ChronoUnit.MILLIS.getDuration();

	public final double refillRatePerMillis;

	public double tokens;
	public long lastRefillTimestamp;
	public Tokenizer<T> tokenizer = $ -> 1;

	public @Nullable ScheduledRunnable scheduledRunnable;

	public RateLimiter(double refillRatePerMillis) {
		this.refillRatePerMillis = refillRatePerMillis;
		this.lastRefillTimestamp = reactor.currentTimeMillis();
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

		public Builder withInitialTokens(double initialTokens) {
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
	protected Promise<Void> onItem(T item) {
		scheduledRunnable = null;

		refill();

		double itemTokens = tokenizer.getTokens(item);
		if (itemTokens <= tokens) {
			tokens -= itemTokens;
			return send(item);
		}

		return Promise.ofCallback(cb ->
				scheduledRunnable = reactor.delay(
						calculateDelay(itemTokens),
						() -> onItem(item)
								.whenComplete(cb::set))
		);
	}

	private void refill() {
		long timestamp = reactor.currentTimeMillis();
		double passedMillis = timestamp - lastRefillTimestamp;

		tokens += passedMillis * refillRatePerMillis;
		lastRefillTimestamp = timestamp;
	}

	private long calculateDelay(double itemTokens) {
		double missing = itemTokens - tokens;
		assert missing > 0;

		return (long) Math.ceil(missing / refillRatePerMillis);
	}

	@Override
	protected void onCleanup() {
		scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
	}

	public interface Tokenizer<T> {
		double getTokens(T item);

		static Tokenizer<ByteBuf> forByteBufs() {
			return ByteBuf::readRemaining;
		}
	}
}
