package io.activej.jmx.stats;

import org.junit.Test;

import static io.activej.jmx.stats.JmxHistograms.integerLogBase10;
import static org.junit.Assert.assertEquals;

public class JmxHistogramsTest {

	@Test
	public void testLogBase10() {
		assertEquals(0, integerLogBase10(1));
		assertEquals(0, integerLogBase10(10 - 1));
		assertEquals(1, integerLogBase10(10 + 1));
		assertEquals(1, integerLogBase10(10));
		assertEquals(1, integerLogBase10(100 - 1));
		assertEquals(2, integerLogBase10(100 + 1));
		assertEquals(2, integerLogBase10(100));
		assertEquals(2, integerLogBase10(1000 - 1));
		assertEquals(3, integerLogBase10(1000 + 1));
		assertEquals(3, integerLogBase10(1000));
		assertEquals(3, integerLogBase10(10000 - 1));
		assertEquals(4, integerLogBase10(10000 + 1));
		assertEquals(4, integerLogBase10(10000));
		assertEquals(4, integerLogBase10(100000 - 1));
		assertEquals(5, integerLogBase10(100000 + 1));
		assertEquals(5, integerLogBase10(100000));
		assertEquals(5, integerLogBase10(1000000 - 1));
		assertEquals(6, integerLogBase10(1000000 + 1));
		assertEquals(6, integerLogBase10(1000000));
		assertEquals(6, integerLogBase10(10000000 - 1));
		assertEquals(7, integerLogBase10(10000000 + 1));
		assertEquals(7, integerLogBase10(10000000));
		assertEquals(7, integerLogBase10(100000000 - 1));
		assertEquals(8, integerLogBase10(100000000 + 1));
		assertEquals(8, integerLogBase10(100000000));
		assertEquals(8, integerLogBase10(1000000000 - 1));
		assertEquals(9, integerLogBase10(1000000000 + 1));
		assertEquals(9, integerLogBase10(1000000000));
		assertEquals(9, integerLogBase10(Integer.MAX_VALUE));

	}

	@Test
	public void testBase2() {
		JmxHistograms.Base2 stats = new JmxHistograms.Base2();
		stats.record(-1);
		assertEquals(1, stats.counts()[0]);
		stats.record(0);
		assertEquals(1, stats.counts()[1]);
		stats.record(Integer.MAX_VALUE);
		assertEquals(1, stats.counts()[stats.counts().length - 1]);
	}

	@Test
	public void testBase10() {
		JmxHistograms.Base10 stats = new JmxHistograms.Base10();
		stats.record(-1);
		assertEquals(1, stats.counts()[0]);
		stats.record(0);
		assertEquals(1, stats.counts()[1]);
		stats.record(Integer.MAX_VALUE);
		assertEquals(1000000000, stats.levels()[10]);
		assertEquals(1, stats.counts()[11]);
	}

	@Test
	public void testBase10Linear() {
		JmxHistograms.Base10Linear stats = new JmxHistograms.Base10Linear();
		stats.record(-1);
		assertEquals(1, stats.counts()[0]);
		stats.record(0);
		assertEquals(1, stats.counts()[1]);
		stats.record(1);
		assertEquals(1, stats.counts()[2]);
		stats.record(2);
		assertEquals(1, stats.counts()[3]);
		stats.record(9);
		assertEquals(1, stats.counts()[10]);
		stats.record(10);
		assertEquals(1, stats.counts()[11]);
		stats.record(11);
		assertEquals(2, stats.counts()[11]);
		stats.record(Integer.MAX_VALUE);
		assertEquals(2000000000, stats.levels()[stats.counts().length - 2]);
		assertEquals(1, stats.counts()[stats.counts().length - 1]);
	}

}
