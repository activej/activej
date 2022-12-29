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

import static io.activej.common.Utils.nullify;

public final class ChannelRateLimiter<T> extends AbstractChannelTransformer<ChannelRateLimiter<T>, T, T>
		implements WithInitializer<ChannelRateLimiter<T>> {
	private final long refillRatePerSecond;

	private long tokens;
	private long lastRefillTimestamp;
	private Tokenizer<T> tokenizer = $ -> 1;

	private @Nullable ScheduledRunnable scheduledRunnable;

	private ChannelRateLimiter(long refillRatePerSecond) {
		this.refillRatePerSecond = refillRatePerSecond;
		this.lastRefillTimestamp = reactor.currentTimeMillis();
	}

	public static <T> ChannelRateLimiter<T> create(long refillRatePerSecond) {
		return new ChannelRateLimiter<>(refillRatePerSecond);
	}

	public ChannelRateLimiter<T> withInitialTokens(long initialTokens) {
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

		long itemTokens = tokenizer.getTokens(item);
		if (itemTokens <= tokens) {
			tokens -= itemTokens;
			return send(item);
		}

		return Promise.ofCallback(cb -> scheduledRunnable = reactor.delay(calculateDelay(itemTokens), () -> onItem(item).whenComplete(cb::accept)));
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

	@Override
	protected void onCleanup() {
		scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
	}

	public interface Tokenizer<T> {
		long getTokens(T item);

		static Tokenizer<ByteBuf> forByteBufs() {
			return ByteBuf::readRemaining;
		}
	}
}
