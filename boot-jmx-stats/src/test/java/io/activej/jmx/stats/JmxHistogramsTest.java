package io.activej.jmx.stats;

import org.junit.Test;

import static io.activej.jmx.stats.JmxHistograms.longLogBase10;
import static org.junit.Assert.assertEquals;

public class JmxHistogramsTest {

	@Test
	public void testLogBase10() {
		assertEquals(0, longLogBase10(1));
		assertEquals(0, longLogBase10(10 - 1));
		assertEquals(1, longLogBase10(10 + 1));
		assertEquals(1, longLogBase10(10));
		assertEquals(1, longLogBase10(100 - 1));
		assertEquals(2, longLogBase10(100 + 1));
		assertEquals(2, longLogBase10(100));
		assertEquals(2, longLogBase10(1000 - 1));
		assertEquals(3, longLogBase10(1000 + 1));
		assertEquals(3, longLogBase10(1000));
		assertEquals(3, longLogBase10(10000 - 1));
		assertEquals(4, longLogBase10(10000 + 1));
		assertEquals(4, longLogBase10(10000));
		assertEquals(4, longLogBase10(100000 - 1));
		assertEquals(5, longLogBase10(100000 + 1));
		assertEquals(5, longLogBase10(100000));
		assertEquals(5, longLogBase10(1000000 - 1));
		assertEquals(6, longLogBase10(1000000 + 1));
		assertEquals(6, longLogBase10(1000000));
		assertEquals(6, longLogBase10(10000000 - 1));
		assertEquals(7, longLogBase10(10000000 + 1));
		assertEquals(7, longLogBase10(10000000));
		assertEquals(7, longLogBase10(100000000 - 1));
		assertEquals(8, longLogBase10(100000000 + 1));
		assertEquals(8, longLogBase10(100000000));
		assertEquals(8, longLogBase10(1000000000 - 1));
		assertEquals(9, longLogBase10(1000000000 + 1));
		assertEquals(9, longLogBase10(1000000000));
		assertEquals(9, longLogBase10(10000000000L - 1));
		assertEquals(10, longLogBase10(10000000000L + 1));
		assertEquals(10, longLogBase10(10000000000L));
		assertEquals(10, longLogBase10(100000000000L - 1));
		assertEquals(11, longLogBase10(100000000000L + 1));
		assertEquals(11, longLogBase10(100000000000L));
		assertEquals(11, longLogBase10(1000000000000L - 1));
		assertEquals(12, longLogBase10(1000000000000L + 1));
		assertEquals(12, longLogBase10(1000000000000L));
		assertEquals(12, longLogBase10(10000000000000L - 1));
		assertEquals(13, longLogBase10(10000000000000L + 1));
		assertEquals(13, longLogBase10(10000000000000L));
		assertEquals(13, longLogBase10(100000000000000L - 1));
		assertEquals(14, longLogBase10(100000000000000L + 1));
		assertEquals(14, longLogBase10(100000000000000L));
		assertEquals(14, longLogBase10(1000000000000000L - 1));
		assertEquals(15, longLogBase10(1000000000000000L + 1));
		assertEquals(15, longLogBase10(1000000000000000L));
		assertEquals(15, longLogBase10(10000000000000000L - 1));
		assertEquals(16, longLogBase10(10000000000000000L + 1));
		assertEquals(16, longLogBase10(10000000000000000L));
		assertEquals(16, longLogBase10(100000000000000000L - 1));
		assertEquals(17, longLogBase10(100000000000000000L + 1));
		assertEquals(17, longLogBase10(100000000000000000L));
		assertEquals(17, longLogBase10(1000000000000000000L - 1));
		assertEquals(18, longLogBase10(1000000000000000000L + 1));
		assertEquals(18, longLogBase10(1000000000000000000L));
		assertEquals(18, longLogBase10(Long.MAX_VALUE));
	}

	@Test
	public void testBase2() {
		JmxHistograms.Base2 stats = new JmxHistograms.Base2();
		stats.record(-1);
		assertEquals(1, stats.counts()[0]);
		stats.record(0);
		assertEquals(1, stats.counts()[1]);
		stats.record(Long.MAX_VALUE);
		assertEquals(1, stats.counts()[stats.counts().length - 1]);
	}

	@Test
	public void testBase10() {
		JmxHistograms.Base10 stats = new JmxHistograms.Base10();
		stats.record(-1);
		assertEquals(1, stats.counts()[0]);
		stats.record(0);
		assertEquals(1, stats.counts()[1]);
		stats.record(Long.MAX_VALUE);
		assertEquals(1, stats.counts()[stats.counts().length - 1]);
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
		stats.record(Long.MAX_VALUE);
		assertEquals(1, stats.counts()[stats.counts().length - 1]);
	}

}
