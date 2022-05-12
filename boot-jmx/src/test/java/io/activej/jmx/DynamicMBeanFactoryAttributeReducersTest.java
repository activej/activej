package io.activej.jmx;

import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducer;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import org.junit.Test;

import javax.management.DynamicMBean;
import java.util.List;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static org.junit.Assert.assertEquals;

public class DynamicMBeanFactoryAttributeReducersTest {

	// region simple type reducers
	@Test
	public void createdMBeanShouldUseSpecifiedReducerForAggregation() throws Exception {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(
						List.of(new MBeanWithCustomReducer(200), new MBeanWithCustomReducer(350)),
						defaultSettings(),
						false);

		assertEquals(ConstantValueReducer.CONSTANT_VALUE, mbean.getAttribute("attr"));
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanWithCustomReducer {
		private final int attr;

		public MBeanWithCustomReducer(int attr) {
			this.attr = attr;
		}

		@JmxAttribute(reducer = ConstantValueReducer.class)
		public int getAttr() {
			return attr;
		}
	}

	public static final class ConstantValueReducer implements JmxReducer<Object> {
		public static final int CONSTANT_VALUE = 10;

		@Override
		public Object reduce(List<?> list) {
			return CONSTANT_VALUE;
		}
	}
	// simple type reducers

	// region pojo reducers
	@Test
	public void properlyAggregatesPojosWithReducer() throws Exception {
		MBeanWithPojoReducer mbean_1 = new MBeanWithPojoReducer(new PojoStub(10, "abc"));
		MBeanWithPojoReducer mbean_2 = new MBeanWithPojoReducer(new PojoStub(15, "xz"));
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(
						List.of(mbean_1, mbean_2),
						defaultSettings(),
						false);

		assertEquals(25, mbean.getAttribute("pojo_count"));
		assertEquals("abcxz", mbean.getAttribute("pojo_name"));
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanWithPojoReducer {
		private final PojoStub pojo;

		public MBeanWithPojoReducer(PojoStub pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute(reducer = PojoStubReducer.class)
		public PojoStub getPojo() {
			return pojo;
		}

	}

	public static final class PojoStub {
		private final int count;
		private final String name;

		public PojoStub(int count, String name) {
			this.count = count;
			this.name = name;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}

		@JmxAttribute
		public String getName() {
			return name;
		}
	}

	public static final class PojoStubReducer implements JmxReducer<PojoStub> {
		@Override
		public PojoStub reduce(List<? extends PojoStub> list) {
			int totalCount = 0;
			StringBuilder totalName = new StringBuilder();

			for (PojoStub pojoStub : list) {
				totalCount += pojoStub.getCount();
				totalName.append(pojoStub.getName());
			}

			return new PojoStub(totalCount, totalName.toString());
		}
	}
	// endregion
}
