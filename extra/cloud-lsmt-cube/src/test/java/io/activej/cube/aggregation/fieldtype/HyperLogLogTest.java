package io.activej.cube.aggregation.fieldtype;

import io.activej.cube.aggregation.util.HyperLogLog;
import org.junit.Test;

import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HyperLogLogTest {
	@Test
	public void test() {
		doTest(0, 64, 0);
		doTest(0, 128, 0);
		doTest(0, 256, 0);

		doTest(1, 64, 0);
		doTest(1, 128, 0);
		doTest(1, 256, 0);

		doTest(10, 64, 0.07);
		doTest(10, 128, 0.04);
		doTest(10, 256, 0.02);

		doTest(100, 64, 0.10);
		doTest(100, 128, 0.07);
		doTest(100, 256, 0.05);

		doTest(1_000, 64, 0.11);
		doTest(1_000, 128, 0.10);
		doTest(1_000, 256, 0.06);

//		doTest(1_000_000, 64, 0.12);
//		doTest(1_000_000, 128, 0.10);
//		doTest(1_000_000, 256, 0.06);

//		doTest(1_000_000_000, 64, 0.08);
//		doTest(1_000_000_000, 128, 0.07);
//		doTest(1_000_000_000, 256, 0.04);
	}

	@Test
	public void testAddInt() {
		HyperLogLog hyperLogLog = new HyperLogLog(64);
		int actual = 1000;
		for (int i = 0; i < actual; i++) {
			hyperLogLog.addInt(i);
		}
		int estimated = hyperLogLog.estimate();
		double error = estimated - actual == 0 ? 0 : Math.abs((double) (estimated - actual) / actual);
		assertTrue(error < 0.1);
	}

	private static final int TEST_RUNS = 100;

	private void doTest(int actualItems, int buckets, double expectedError) {
		System.out.println("actualItems: " + actualItems + ", buckets: " + buckets);

		double avgError = 0;
		double maxError = 0;
		double avgEstimatedItems = 0;

		for (int i = 0; i < TEST_RUNS; i++) {
			HyperLogLog hyperLogLog = new HyperLogLog(buckets);
			long start = (long) actualItems * i;
			for (long v = start; v < start + actualItems; v++) {
				hyperLogLog.addLong(v);
			}
			int estimatedItems = hyperLogLog.estimate();
			double error = estimatedItems - actualItems == 0 ?
				0 :
				(double) (estimatedItems - actualItems) / actualItems;
			if (actualItems >= 100) {
				System.out.println("> estimatedItems: " + estimatedItems + ", error: " + error);
			}
			if (abs(error) > abs(maxError))
				maxError = error;
			avgError += abs(error);
			avgEstimatedItems += estimatedItems;
		}
		avgError /= TEST_RUNS;
		avgEstimatedItems /= TEST_RUNS;
		System.out.println("  estimatedItems: " + avgEstimatedItems + " error: " + avgError + ", maxError: " + maxError);

		assertEquals(0, avgError, expectedError);
	}
}
