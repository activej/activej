package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.JmxRefreshableStats;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static java.util.Arrays.asList;

public class DynamicMBeanFactoryAttributeExceptionsTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void concurrentJmxBeansAreNotAllowedToBeInPool() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("ConcurrentJmxBeans cannot be used in pool");

		DynamicMBeanFactory.create()
				.createDynamicMBean(
						asList(new ConcurrentJmxBeanWithSingleIntAttr(), new ConcurrentJmxBeanWithSingleIntAttr()),
						defaultSettings(), false);
	}

	// test JmxRefreshableStats as @JmxAttribute, all returned stats should be concrete classes with public no-arg constructor
	@Test
	public void jmxStatsAttributeCannotBeInterface() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor");

		DynamicMBeanFactory.create()
				.createDynamicMBean(asList(new MBeanWithInterfaceAsJmxStatsAttributes()), defaultSettings(), false);
	}

	@Test
	public void jmxStatsAttributeCannotBeAbstractClass() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor");

		DynamicMBeanFactory.create()
				.createDynamicMBean(asList(new MBeanWithAbstractClassAsJmxStatsAttributes()), defaultSettings(), false);
	}

	@Test
	public void jmxStatsAttributesClassMustHavePublicNoArgConstructor() {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Return type of JmxRefreshableStats attribute must be concrete class " +
				"that implements JmxRefreshableStats interface " +
				"and contains static factory \"createAccumulator()\" method " +
				"or static factory \"create()\" method " +
				"or public no-arg constructor");

		DynamicMBeanFactory.create()
				.createDynamicMBean(asList(new MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor()), defaultSettings(), false);
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
