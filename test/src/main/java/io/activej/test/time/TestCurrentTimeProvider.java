package io.activej.test.time;

import io.activej.common.time.CurrentTimeProvider;

public class TestCurrentTimeProvider {
	public static TimeSequenceCurrentTimeProvider ofTimeSequence(long start, long increment) {
		return new TimeSequenceCurrentTimeProvider(start, increment);
	}

	public static ConstantCurrentTimeProvider ofConstant(long time) {
		return new ConstantCurrentTimeProvider(time);
	}

	public static SettableCurrentTimeProvider settable(CurrentTimeProvider timeProvider) {
		return new SettableCurrentTimeProvider(timeProvider);
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

	public static class SettableCurrentTimeProvider implements CurrentTimeProvider {
		private CurrentTimeProvider timeProvider;

		private SettableCurrentTimeProvider(CurrentTimeProvider timeProvider) {
			this.timeProvider = timeProvider;
		}

		public void setTimeProvider(CurrentTimeProvider timeProvider) {
			this.timeProvider = timeProvider;
		}

		public CurrentTimeProvider getTimeProvider() {
			return timeProvider;
		}

		@Override
		public long currentTimeMillis() {
			return timeProvider.currentTimeMillis();
		}
	}

	public static class ConstantCurrentTimeProvider implements CurrentTimeProvider {
		private final long time;

		private ConstantCurrentTimeProvider(long time) {
			this.time = time;
		}

		@Override
		public long currentTimeMillis() {
			return time;
		}
	}
}
