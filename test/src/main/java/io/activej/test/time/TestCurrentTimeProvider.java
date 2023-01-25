package io.activej.test.time;

import io.activej.common.time.CurrentTimeProvider;

public class TestCurrentTimeProvider {
	public static CurrentTimeProvider_TimeSequence ofTimeSequence(long start, long increment) {
		return new CurrentTimeProvider_TimeSequence(start, increment);
	}

	public static class CurrentTimeProvider_TimeSequence implements CurrentTimeProvider {
		private final long increment;
		long time;

		private CurrentTimeProvider_TimeSequence(long start, long increment) {
			this.increment = increment;
			time = start;
		}

		@Override
		public long currentTimeMillis() {
			long time = this.time;
			this.time += increment;
			return time;
		}

		public long getTime() {
			return time;
		}
	}
}
