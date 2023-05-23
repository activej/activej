package io.activej.jmx;

import io.activej.common.StringFormatUtils;
import io.activej.jmx.DynamicMBeanFactory.JmxCustomTypeAdapter;
import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxReducer;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerMax;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import org.junit.Test;

import javax.management.DynamicMBean;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DynamicMBeanFactoryAttributeReducersTest {

	// region simple type reducers
	@Test
	public void createdMBeanShouldUseSpecifiedReducerForAggregation() throws Exception {
		DynamicMBean mbean = DynamicMBeanFactory.create()
			.createDynamicMBean(
				List.of(new MBeanWithCustomReducer(200), new MBeanWithCustomReducer(350)),
				JmxBeanSettings.create(),
				false);

		assertEquals(ConstantValueReducer.CONSTANT_VALUE, mbean.getAttribute("attr"));
		assertEquals(ConstantValueReducer.CONSTANT_VALUE, mbean.invoke("getOp", new String[0], new String[0]));
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

		@JmxOperation(reducer = ConstantValueReducer.class)
		public int getOp() {
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
	// endregion

	// region custom type reducers
	@Test
	public void createdCustomMBeanShouldUseSpecifiedReducerForAggregation() throws Exception {
		DynamicMBean mbean = DynamicMBeanFactory.create()
			.createDynamicMBean(
				List.of(
					new CustomMBeanWithCustomReducer(200),
					new CustomMBeanWithCustomReducer(350),
					new CustomMBeanWithCustomReducer(100)
				),
				JmxBeanSettings.builder()
					.withCustomTypes(Collections.singletonMap(
						Duration.class,
						new JmxCustomTypeAdapter<>(
							StringFormatUtils::formatDuration,
							StringFormatUtils::parseDuration
						)
					))
					.build(),
				false);

		String maxDuration = StringFormatUtils.formatDuration(Duration.ofSeconds(350));

		assertEquals(maxDuration, mbean.getAttribute("attr"));
		assertEquals(maxDuration, mbean.invoke("getOp", new String[0], new String[0]));
		assertEquals(maxDuration, mbean.invoke("retrieveDuration", new String[0], new String[0]));
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class CustomMBeanWithCustomReducer {
		private final int seconds;

		public CustomMBeanWithCustomReducer(int seconds) {
			this.seconds = seconds;
		}

		@JmxAttribute(reducer = JmxReducerMax.class)
		public Duration getAttr() {
			return Duration.ofSeconds(seconds);
		}

		@JmxOperation(reducer = JmxReducerMax.class)
		public Duration getOp() {
			return Duration.ofSeconds(seconds);
		}

		@JmxOperation(reducer = JmxReducerMax.class)
		public Duration retrieveDuration() {
			return Duration.ofSeconds(seconds);
		}
	}
	// endregion

	// region pojo reducers
	@Test
	public void properlyAggregatesPojosWithReducer() throws Exception {
		MBeanWithPojoReducer mbean_1 = new MBeanWithPojoReducer(new PojoStub(10, "abc"));
		MBeanWithPojoReducer mbean_2 = new MBeanWithPojoReducer(new PojoStub(15, "xz"));
		DynamicMBean mbean = DynamicMBeanFactory.create()
			.createDynamicMBean(
				List.of(mbean_1, mbean_2),
				JmxBeanSettings.create(),
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

	@Test
	public void properlyAggregatesJmxOperationResults() throws Exception {
		DynamicMBean mbean = DynamicMBeanFactory.create()
			.createDynamicMBean(
				List.of(new MBeanWithCustomReducer(200), new MBeanWithCustomReducer(350)),
				JmxBeanSettings.create(),
				false);

		assertEquals(ConstantValueReducer.CONSTANT_VALUE, mbean.invoke("getOp", new String[0], new String[0]));
	}
	// endregion
}
