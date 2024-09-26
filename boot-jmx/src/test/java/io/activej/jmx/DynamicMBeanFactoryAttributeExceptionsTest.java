package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.JmxRefreshableStats;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DynamicMBeanFactoryAttributeExceptionsTest {

	private static final JmxBeanSettings SETTINGS = JmxBeanSettings.create();

	@Test
	public void concurrentJmxBeansAreNotAllowedToBeInPool() {
		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<ConcurrentJmxBeanWithSingleIntAttr> beans = List.of(new ConcurrentJmxBeanWithSingleIntAttr(), new ConcurrentJmxBeanWithSingleIntAttr());

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> dynamicMBeanFactory.createDynamicMBean(beans, SETTINGS, false));
		assertEquals("ConcurrentJmxBeans cannot be used in pool", e.getMessage());
	}

	// test JmxRefreshableStats as @JmxAttribute, all returned stats should be concrete classes with public no-arg constructor
	@Test
	public void jmxStatsAttributeCannotBeInterface() {
		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<MBeanWithInterfaceAsJmxStatsAttributes> beans = List.of(new MBeanWithInterfaceAsJmxStatsAttributes());

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> dynamicMBeanFactory.createDynamicMBean(beans, SETTINGS, false));
		assertTrue(e.getMessage().startsWith(
			"Return type of JmxStats attribute must be a concrete class " +
			"that implements JmxStats interface " +
			"and contains static factory \"createAccumulator()\" method " +
			"or static factory \"create()\" method " +
			"or public no-arg constructor"));
	}

	@Test
	public void jmxStatsAttributeCannotBeAbstractClass() {
		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<MBeanWithAbstractClassAsJmxStatsAttributes> beans = List.of(new MBeanWithAbstractClassAsJmxStatsAttributes());

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> dynamicMBeanFactory.createDynamicMBean(beans, SETTINGS, false));
		assertTrue(e.getMessage().startsWith(
			"Return type of JmxStats attribute must be a concrete class " +
			"that implements JmxStats interface " +
			"and contains static factory \"createAccumulator()\" method " +
			"or static factory \"create()\" method " +
			"or public no-arg constructor"));
	}

	@Test
	public void jmxStatsAttributesClassMustHavePublicNoArgConstructor() {
		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor> beans = List.of(new MBeanWithJmxStatsClassWhichDoesntHavePublicNoArgConstructor());

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> dynamicMBeanFactory.createDynamicMBean(beans, SETTINGS, false));
		assertTrue(e.getMessage().startsWith(
			"Return type of JmxStats attribute must be a concrete class " +
			"that implements JmxStats interface " +
			"and contains static factory \"createAccumulator()\" method " +
			"or static factory \"create()\" method " +
			"or public no-arg constructor"));
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
