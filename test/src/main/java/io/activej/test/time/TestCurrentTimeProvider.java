package io.activej.test.time;

import io.activej.common.Checks;
import io.activej.common.time.CurrentTimeProvider;

public class TestCurrentTimeProvider {
	private static final ThreadLocal<CurrentTimeProvider> THREAD_LOCAL_TIME_PROVIDER = new ThreadLocal<>();

	public static CurrentTimeProvider ofTimeSequence(long start, long increment) {
		return new CurrentTimeProvider() {
			long time = start;

			@Override
			public long currentTimeMillis() {
				long time = this.time;
				this.time += increment;
				return time;
			}
		};
	}

	public static CurrentTimeProvider ofThreadLocal() {
		return Checks.checkNotNull(THREAD_LOCAL_TIME_PROVIDER.get(), "ThreadLocal has no instance of CurrentTimeProvider associated with current thread");
	}
}
