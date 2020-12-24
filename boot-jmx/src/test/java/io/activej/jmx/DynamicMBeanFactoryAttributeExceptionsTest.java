package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.JmxRefreshableStats;
import org.junit.Test;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertThrows;

public class DynamicMBeanFactoryAttributeExceptionsTest {
	@Test
	public void concurrentJmxBeansAreNotAllowedToBeInPool() {
		assertThrows("ConcurrentJmxBeans cannot be used in pool", IllegalArgumentException.class, () ->
				DynamicMBeanFactory.create()
						.createDynamicMBean(
								asList(new ConcurrentJmxBeanWithSingleIntAttr(), new ConcurrentJmxBeanWithSingleIntAttr()),
								defaultSettings(), false));
	}

	// test JmxRefreshableStats as @JmxAttribute, all returned stats should be concrete classes with public no-arg constructor
	@Test
	public void jmxStatsAttributeCannotBeInterface() {
		assertThrows("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor", IllegalArgumentException.class, () ->
				DynamicMBeanFactory.create()
						.createDynamicMBean(singletonList(new MBeanWithInterfaceAsJmxStatsAttributes()), defaultSettings(), false));
	}

	@Test
	public void jmxStatsAttributeCannotBeAbstractClass() {
		assertThrows("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor", IllegalArgumentException.class, () ->
				DynamicMBeanFactory.create()
						.createDynamicMBean(singletonList(new MBeanWithAbstractClassAsJmxStatsAttributes()), defaultSettings(), false));
	}

	@Test
	public void jmxStatsAttributesClassMustHavePublicNoArgConstructor() {
		assertThrows("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor", IllegalArgumentException.class, () ->
				DynamicMBeanFactory.create()
						.createDynamicMBean(singletonList(new MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor()), defaultSettings(), false));
	}

	public static final class ConcurrentJmxBeanWithSingleIntAttr implements ConcurrentJmxBean {

		@JmxAttribute
		public int getCount() {
			return 0;
		}
	}

	public static final class MBeanWithInterfaceAsJmxStatsAttributes implements ConcurrentJmxBean {

		@JmxAttribute
		public JmxStatsAdditionalInterface getStats() {
			return null;
		}
	}

	public interface JmxStatsAdditionalInterface extends JmxRefreshableStats<JmxStatsAdditionalInterface> {
	}

	public static final class MBeanWithAbstractClassAsJmxStatsAttributes implements ConcurrentJmxBean {

		@JmxAttribute
		public JmxStatsAbstractClass getStats() {
			return null;
		}
	}

	public static abstract class JmxStatsAbstractClass implements JmxRefreshableStats<JmxStatsAbstractClass> {

	}

	public static final class MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor implements ConcurrentJmxBean {

		@JmxAttribute
		public JmxStatsWithNoPublicNoArgConstructor getStats() {
			return null;
		}
	}

	public static final class JmxStatsWithNoPublicNoArgConstructor
			implements JmxRefreshableStats<JmxStatsWithNoPublicNoArgConstructor> {
		public JmxStatsWithNoPublicNoArgConstructor(int count) {
		}

		@JmxAttribute
		public int getAttr() {
			return 0;
		}

		@Override
		public void add(JmxStatsWithNoPublicNoArgConstructor another) {

		}

		@Override
		public void refresh(long timestamp) {

		}
	}
}
