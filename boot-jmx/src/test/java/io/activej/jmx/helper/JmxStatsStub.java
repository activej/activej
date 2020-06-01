package io.activej.jmx.helper;

import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.JmxRefreshableStats;

public final class JmxStatsStub implements JmxRefreshableStats<JmxStatsStub> {

	private long sum = 0L;
	private int count = 0;

	public void recordValue(long value) {
		sum += value;
		++count;
	}

	@JmxAttribute
	public long getSum() {
		return sum;
	}

	@JmxAttribute
	public int getCount() {
		return count;
	}

	@Override
	public void add(JmxStatsStub stats) {
		this.sum += stats.sum;
		this.count += stats.count;
	}

	@Override
	public void refresh(long timestamp) {
	}
}
