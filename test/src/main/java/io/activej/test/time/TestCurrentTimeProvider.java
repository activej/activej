package io.activej.test.time;

import io.activej.common.time.CurrentTimeProvider;

public class TestCurrentTimeProvider {
	public static TimeSequenceCurrentTimeProvider ofTimeSequence(long start, long increment) {
		return new TimeSequenceCurrentTimeProvider(start, increment);
	}

	public static class TimeSequenceCurrentTimeProvider implements CurrentTimeProvider {
		private final long increment;
		long time;

		private TimeSequenceCurrentTimeProvider(long start, long increment) {
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
