package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import io.activej.jmx.stats.JmxRefreshableStats;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.List;
import java.util.Map;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static io.activej.jmx.helper.Utils.nameToAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicMBeanFactoryAttributesHidingTest {
	// region pojos
	@Test
	public void omitsNullPojoAttributesInSingleton() {
		MBeanStubOne singletonWithNullPojo = new MBeanStubOne(null);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(singletonWithNullPojo), defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void omitsPojoAttributesInWorkersIfAllWorkersReturnNull() {
		MBeanStubOne mbean_1 = new MBeanStubOne(null);
		MBeanStubOne mbean_2 = new MBeanStubOne(null);
		List<MBeanStubOne> workersWithNullPojo = List.of(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void doesNotOmitPojoAttributesInWorkersIfAtLeastOneWorkerReturnsNonNull() {
		MBeanStubOne mbean_1 = new MBeanStubOne(null);
		MBeanStubOne mbean_2 = new MBeanStubOne(new PojoStub());
		List<MBeanStubOne> workersWithNullPojo = List.of(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(2, attrs.size());
		assertTrue(attrs.containsKey("pojoStub_number"));
		assertTrue(attrs.containsKey("pojoStub_text"));
	}

	public static final class PojoStub {
		@JmxAttribute
		public int getNumber() {
			return 5;
		}

		@JmxAttribute
		public String getText() {
			return "text";
		}
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanStubOne {
		private final PojoStub pojoStub;

		public MBeanStubOne(PojoStub pojoStub) {
			this.pojoStub = pojoStub;
		}

		@JmxAttribute
		public PojoStub getPojoStub() {
			return pojoStub;
		}
	}
	// endregion

	// region jmx stats
	@Test
	public void omitsNullJmxStatsAttributesInSingleton() {
		MBeanStubTwo singletonWithNullJmxStats = new MBeanStubTwo(null);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(singletonWithNullJmxStats), defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void omitsJmxStatsAttributesInWorkersIfAllWorkersReturnNull() {
		MBeanStubTwo mbean_1 = new MBeanStubTwo(null);
		MBeanStubTwo mbean_2 = new MBeanStubTwo(null);
		List<MBeanStubTwo> workersWithNullPojo = List.of(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(0, attrs.size());
	}

	@Test
	public void doesNotOmitJmxStatsAttributesInWorkersIfAtLeastOneWorkerReturnsNonNull() {
		MBeanStubTwo mbean_1 = new MBeanStubTwo(null);
		MBeanStubTwo mbean_2 = new MBeanStubTwo(new JmxStatsStub());
		List<MBeanStubTwo> workersWithNullPojo = List.of(mbean_1, mbean_2);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(workersWithNullPojo, defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(1, attrs.size());
		assertTrue(attrs.containsKey("jmxStatsStub_value"));
	}

	public static final class JmxStatsStub implements JmxRefreshableStats<JmxStatsStub> {
		@JmxAttribute
		public double getValue() {
			return 5;
		}

		@Override
		public void refresh(long timestamp) {
		}

		@Override
		public void add(JmxStatsStub another) {
		}
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanStubTwo {
		private final JmxStatsStub jmxStatsStub;

		public MBeanStubTwo(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStatsStub() {
			return jmxStatsStub;
		}
	}
	// endregion

	// region stats in pojo
	@Test
	public void omitsNullPojosInNonNullPojos() {
		MBeanStubThree bean = new MBeanStubThree(new PojoStubThree(null));
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(bean), defaultSettings(), false);

		MBeanInfo mbeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbeanInfo.getAttributes());

		assertEquals(1, attrs.size());
		assertTrue(attrs.containsKey("pojoStubThree_number"));
	}

	public static final class PojoStubThree {
		private final JmxStatsStub jmxStatsStub;

		public PojoStubThree(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStats() {
			return jmxStatsStub;
		}

		@JmxAttribute
		public int getNumber() {
			return 10;
		}
	}

	public static final class MBeanStubThree implements ConcurrentJmxBean {
		private final PojoStubThree pojoStubThree;

		public MBeanStubThree(PojoStubThree pojoStubThree) {
			this.pojoStubThree = pojoStubThree;
		}

		@JmxAttribute
		public PojoStubThree getPojoStubThree() {
			return pojoStubThree;
		}
	}
	// endregion
}
