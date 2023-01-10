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

package io.activej.csp.process;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.initializer.WithInitializer;
import io.activej.promise.Promise;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nullify;

public final class ChannelRateLimiter<T> extends AbstractChannelTransformer<ChannelRateLimiter<T>, T, T>
		implements WithInitializer<ChannelRateLimiter<T>> {
	private static final Duration MILLIS_DURATION = ChronoUnit.MILLIS.getDuration();

	private final double refillRatePerMillis;

	private double tokens;
	private long lastRefillTimestamp;
	private Tokenizer<T> tokenizer = $ -> 1;

	private @Nullable ScheduledRunnable scheduledRunnable;

	private ChannelRateLimiter(double refillRatePerMillis) {
		this.refillRatePerMillis = refillRatePerMillis;
		this.lastRefillTimestamp = reactor.currentTimeMillis();
	}

	public static <T> ChannelRateLimiter<T> create(double refillRate, ChronoUnit perUnit) {
		checkArgument(refillRate >= 0, "Negative refill rate");

		Duration perUnitDuration = perUnit.getDuration();
		double refillRatePerMillis;
		if (perUnit.ordinal() > ChronoUnit.MILLIS.ordinal()) {
			refillRatePerMillis = refillRate / perUnitDuration.dividedBy(MILLIS_DURATION);
		} else {
			refillRatePerMillis = refillRate * MILLIS_DURATION.dividedBy(perUnitDuration);
		}
		return new ChannelRateLimiter<>(refillRatePerMillis);
	}

	public ChannelRateLimiter<T> withInitialTokens(double initialTokens) {
		this.tokens = initialTokens;
		return this;
	}

	public ChannelRateLimiter<T> withTokenizer(Tokenizer<T> tokenizer) {
		this.tokenizer = tokenizer;
		return this;
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
								.whenComplete(cb::accept))
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
