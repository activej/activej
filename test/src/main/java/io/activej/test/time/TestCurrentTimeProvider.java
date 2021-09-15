package io.activej.test.time;

import io.activej.common.time.CurrentTimeProvider;

public class TestCurrentTimeProvider {
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
}
