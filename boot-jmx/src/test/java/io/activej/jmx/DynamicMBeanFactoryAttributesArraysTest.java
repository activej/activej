package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import org.junit.Test;

import javax.management.DynamicMBean;
import java.util.List;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static org.junit.Assert.assertArrayEquals;

public class DynamicMBeanFactoryAttributesArraysTest {

	@Test
	public void properlyGetsArrayOfInts() throws Exception {
		MBeanWithIntArray mBeanWithIntArray = new MBeanWithIntArray();
		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithIntArray);

		Integer[] expected = {1, 2, 3};
		assertArrayEquals(expected, (Integer[]) mbean.getAttribute("integerNumbers"));
	}

	@Test
	public void properlyGetsArrayOfDoubles() throws Exception {
		MBeanWithDoubleArray mBeanWithDoubleArray = new MBeanWithDoubleArray();
		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithDoubleArray);

		Double[] expected = {1.1, 2.2, 3.3};
		assertArrayEquals(expected, (Double[]) mbean.getAttribute("doubleNumbers"));
	}

	@Test
	public void concatsArraysWhileAggregatingSeveralMBeans() throws Exception {
		MBeanWithIntArray mBeanWithIntArray1 = new MBeanWithIntArray();
		MBeanWithIntArray mBeanWithIntArray2 = new MBeanWithIntArray();

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithIntArray1, mBeanWithIntArray2);

		Integer[] expected = {1, 2, 3, 1, 2, 3};
		assertArrayEquals(expected, (Integer[]) mbean.getAttribute("integerNumbers"));
	}

	// region helper classes
	@JmxBean(JmxBeanAdapterStub.class)
	public static class MBeanWithIntArray {

		@JmxAttribute
		public int[] getIntegerNumbers() {
			return new int[]{1, 2, 3};
		}
	}

	public static class MBeanWithDoubleArray implements ConcurrentJmxBean {

		@JmxAttribute
		public double[] getDoubleNumbers() {
			return new double[]{1.1, 2.2, 3.3};
		}
	}
	// endregion

	// region helper methods
	public static DynamicMBean createDynamicMBeanFor(Object... objects) {
		return DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(objects), defaultSettings(), false);
	}
	// endregion
}
