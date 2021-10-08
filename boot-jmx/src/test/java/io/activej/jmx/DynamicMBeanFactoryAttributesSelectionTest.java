package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.JmxRefreshableStats;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.Map;

import static io.activej.jmx.DynamicMBeanFactoryAttributesTest.createDynamicMBeanFor;
import static io.activej.jmx.helper.Utils.nameToAttribute;
import static org.junit.Assert.*;

public class DynamicMBeanFactoryAttributesSelectionTest {

	@Test
	public void doNotConsiderOptionalAttributesByDefault() {
		MBeanWithNoExtraSubAttributes mbeanStub = new MBeanWithNoExtraSubAttributes();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);
		assertEquals("stats_sum", attributesInfoArr[0].getName());

	}

	@Test
	public void considerOptionalAttributesIfTheyAreSpecified() {
		MBeanWithExtraSubAttributes mbeanStub = new MBeanWithExtraSubAttributes();
		DynamicMBean mbean = createDynamicMBeanFor(mbeanStub);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(2, nameToAttr.size());
		assertTrue(nameToAttr.containsKey("stats_sum"));
		assertTrue(nameToAttr.containsKey("stats_count"));

	}

	@Test
	public void throwsExceptionInCaseOfInvalidFieldName() {
		MBeansStubWithInvalidExtraAttrName mbeanStub = new MBeansStubWithInvalidExtraAttrName();

		assertThrows(RuntimeException.class, () -> createDynamicMBeanFor(mbeanStub));
	}

	public static class MBeanWithNoExtraSubAttributes implements ConcurrentJmxBean {
		private final JmxStatsWithOptionalAttributes stats = new JmxStatsWithOptionalAttributes();

		@JmxAttribute
		public JmxStatsWithOptionalAttributes getStats() {
			return stats;
		}
	}

	public static class MBeanWithExtraSubAttributes implements ConcurrentJmxBean {
		private final JmxStatsWithOptionalAttributes stats = new JmxStatsWithOptionalAttributes();

		@JmxAttribute(extraSubAttributes = "count")
		public JmxStatsWithOptionalAttributes getStats() {
			return stats;
		}
	}

	public static class MBeansStubWithInvalidExtraAttrName implements ConcurrentJmxBean {
		private final JmxStatsWithOptionalAttributes stats = new JmxStatsWithOptionalAttributes();

		@JmxAttribute(extraSubAttributes = "QWERTY") // QWERTY subAttribute doesn't exist
		public JmxStatsWithOptionalAttributes getStats() {
			return stats;
		}
	}

	public static class JmxStatsWithOptionalAttributes implements JmxRefreshableStats<JmxStatsWithOptionalAttributes> {

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

		@JmxAttribute(optional = true)
		public int getCount() {
			return count;
		}

		@Override
		public void add(JmxStatsWithOptionalAttributes stats) {
			this.sum += stats.sum;
			this.count += stats.count;
		}

		@Override
		public void refresh(long timestamp) {

		}
	}
}
