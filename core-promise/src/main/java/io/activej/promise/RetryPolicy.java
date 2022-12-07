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

package io.activej.promise;

import io.activej.common.ref.RefInt;
import io.activej.common.ref.RefLong;
import io.activej.common.tuple.Tuple2;

import java.time.Duration;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.*;

public interface RetryPolicy<S> {
	S createRetryState();

	long nextRetryTimestamp(long now, Exception lastError, S retryState);

	abstract class StatelessRetryPolicy implements RetryPolicy<Void> {
		@Override
		public final Void createRetryState() {
			return null;
		}
	}

	static RetryPolicy<Void> noRetry() {
		return new StatelessRetryPolicy() {
			@Override
			public long nextRetryTimestamp(long now, Exception lastError, Void retryState) {
				return 0;
			}
		};
	}

	static RetryPolicy<Void> immediateRetry() {
		return new StatelessRetryPolicy() {
			@Override
			public long nextRetryTimestamp(long now, Exception lastError, Void retryState) {
				return now;
			}
		};
	}

	static RetryPolicy<Void> fixedDelay(Duration delay) {
		return fixedDelay(delay.toMillis());
	}

	static RetryPolicy<Void> fixedDelay(long delay) {
		return new StatelessRetryPolicy() {
			@Override
			public long nextRetryTimestamp(long now, Exception lastError, Void retryState) {
				return now + delay;
			}
		};
	}

	class SimpleRetryState {
		protected Exception lastError;
		protected int retryCount;
		protected long retryFirstTimestamp;
	}

	abstract class SimpleRetryPolicy implements RetryPolicy<SimpleRetryState> {
		@Override
		public final SimpleRetryState createRetryState() {
			return new SimpleRetryState();
		}

		@Override
		public final long nextRetryTimestamp(long now, Exception lastError, SimpleRetryState retryState) {
			retryState.lastError = lastError;
			retryState.retryCount++;
			if (retryState.retryFirstTimestamp == 0L) {
				retryState.retryFirstTimestamp = now;
			}
			return nextRetryTimestamp(now, lastError, retryState.retryCount, retryState.retryFirstTimestamp);
		}

		public abstract long nextRetryTimestamp(long now, Exception lastError, int retryCount, long firstRetryTimestamp);
	}

	static RetryPolicy<?> exponentialBackoff(Duration initialDelay, Duration maxDelay, double exponent) {
		return exponentialBackoff(initialDelay.toMillis(), maxDelay.toMillis(), exponent);
	}

	static RetryPolicy<?> exponentialBackoff(long initialDelay, long maxDelay, double exponent) {
		checkArgument(maxDelay > initialDelay && exponent > 1.0,
				"Max delay should be greater than initial delay and exponent should be greater than 1.0");
		int maxRetryCount = (int) ceil(log((double) maxDelay / initialDelay) / log(exponent));
		return new SimpleRetryPolicy() {
			@Override
			public long nextRetryTimestamp(long now, Exception lastError, int retryCount, long firstRetryTimestamp) {
				return now + (
						retryCount > maxRetryCount ?
								maxDelay :
								min(maxDelay, (long) (initialDelay * pow(exponent, retryCount))));
			}
		};
	}

	static RetryPolicy<?> exponentialBackoff(Duration initialDelay, Duration maxDelay) {
		return exponentialBackoff(initialDelay.toMillis(), maxDelay.toMillis());
	}

	static RetryPolicy<?> exponentialBackoff(long initialDelay, long maxDelay) {
		return exponentialBackoff(initialDelay, maxDelay, 2.0);
	}

	abstract class DelegatingRetryPolicy<S, DS> implements RetryPolicy<Tuple2<S, DS>> {
		private final RetryPolicy<DS> delegateRetryPolicy;

		protected DelegatingRetryPolicy(RetryPolicy<DS> policy) {delegateRetryPolicy = policy;}

		@Override
		public final Tuple2<S, DS> createRetryState() {
			DS delegateRetryState = delegateRetryPolicy.createRetryState();
			S retryState = doCreateRetryState();
			return new Tuple2<>(retryState, delegateRetryState);
		}

		protected abstract S doCreateRetryState();

		@Override
		public final long nextRetryTimestamp(long now, Exception lastError, Tuple2<S, DS> retryState) {
			return nextRetryTimestamp(now, lastError, retryState.value1(), retryState.value2());
		}

		public abstract long nextRetryTimestamp(long now, Exception lastError, S retryState, DS delegateRetryState);
	}

	default RetryPolicy<Tuple2<RefInt, S>> withMaxTotalRetryCount(int maxRetryCount) {
		return new DelegatingRetryPolicy<>(this) {
			@Override
			protected RefInt doCreateRetryState() {
				return new RefInt(0);
			}

			@Override
			public long nextRetryTimestamp(long now, Exception lastError, RefInt retryState, S delegateRetryState) {
				if (retryState.value++ < maxRetryCount) {
					return RetryPolicy.this.nextRetryTimestamp(now, lastError, delegateRetryState);
				} else {
					return 0L;
				}
			}
		};
	}

	default RetryPolicy<?> withMaxTotalRetryTimeout(Duration maxRetryTimeout) {
		long maxRetryTimeoutMillis = maxRetryTimeout.toMillis();
		return new DelegatingRetryPolicy<RefLong, S>(this) {
			@Override
			protected RefLong doCreateRetryState() {
				return new RefLong(0);
			}

			@Override
			public long nextRetryTimestamp(long now, Exception lastError, RefLong retryState, S delegateRetryState) {
				if (retryState.value == 0) retryState.value = now;
				if (now < retryState.value + maxRetryTimeoutMillis) {
					return RetryPolicy.this.nextRetryTimestamp(now, lastError, delegateRetryState);
				} else {
					return 0L;
				}
			}
		};
	}

}
