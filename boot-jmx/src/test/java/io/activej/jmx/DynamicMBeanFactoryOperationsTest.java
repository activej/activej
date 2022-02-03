package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxParameter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public final class DynamicMBeanFactoryOperationsTest {

	@Test
	public void itShouldCollectInformationAboutJMXOperationsToMBeanInfo() {
		BeanStubWithOperations bean = new BeanStubWithOperations();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(bean), defaultSettings(), false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		MBeanOperationInfo[] operations = mBeanInfo.getOperations();
		Map<String, MBeanOperationInfo> nameToOperation = new HashMap<>();
		for (MBeanOperationInfo operation : operations) {
			nameToOperation.put(operation.getName(), operation);
		}

		assertThat(nameToOperation, hasKey("increment"));
		assertThat(nameToOperation, hasKey("addInfo"));
		assertThat(nameToOperation, hasKey("multiplyAndAdd"));

		MBeanOperationInfo incrementOperation = nameToOperation.get("increment");
		MBeanOperationInfo addInfoOperation = nameToOperation.get("addInfo");
		MBeanOperationInfo multiplyAndAddOperation = nameToOperation.get("multiplyAndAdd");

		assertThat(incrementOperation, hasReturnType("void"));

		assertThat(addInfoOperation, hasParameter("information", String.class.getName()));
		assertThat(addInfoOperation, hasReturnType("void"));

		// parameter names are not annotated
		assertThat(multiplyAndAddOperation, hasParameter("arg0", "long"));
		assertThat(multiplyAndAddOperation, hasParameter("arg1", "long"));
		assertThat(multiplyAndAddOperation, hasReturnType("void"));
	}

	@Test
	public void itShouldInvokeAnnotatedOperationsThroughDynamicMBeanInterface() throws Exception {
		BeanStubWithOperations bean = new BeanStubWithOperations();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(bean), defaultSettings(), false);

		mbean.invoke("increment", null, null);
		mbean.invoke("increment", null, null);

		mbean.invoke("addInfo", new Object[]{"data1"}, new String[]{String.class.getName()});
		mbean.invoke("addInfo", new Object[]{"data2"}, new String[]{String.class.getName()});

		mbean.invoke("multiplyAndAdd", new Object[]{120, 150}, new String[]{"long", "long"});

		assertEquals(2, bean.getCount());
		assertEquals("data1data2", bean.getInfo());
		assertEquals(120 * 150, bean.getSum());
	}

	@Ignore("does not work concurrently yet")
	@Test
	public void itShouldBroadcastOperationCallToAllBean() throws Exception {
		BeanStubWithOperations bean_1 = new BeanStubWithOperations();
		BeanStubWithOperations bean_2 = new BeanStubWithOperations();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(bean_1, bean_2), defaultSettings(), false);

		// set manually init value for second bean to be different from first
		bean_2.inc();
		bean_2.inc();
		bean_2.inc();
		bean_2.addInfo("second");
		bean_2.multiplyAndAdd(10, 15);

		mbean.invoke("increment", null, null);
		mbean.invoke("increment", null, null);

		mbean.invoke("addInfo", new Object[]{"data1"}, new String[]{String.class.getName()});
		mbean.invoke("addInfo", new Object[]{"data2"}, new String[]{String.class.getName()});

		mbean.invoke("multiplyAndAdd", new Object[]{120, 150}, new String[]{"long", "long"});

		// check first bean
		assertEquals(2, bean_1.getCount());
		assertEquals("data1data2", bean_1.getInfo());
		assertEquals(120 * 150, bean_1.getSum());

		// check second bean
		assertEquals(2 + 3, bean_2.getCount());
		assertEquals("second" + "data1data2", bean_2.getInfo());
		assertEquals(10 * 15 + 120 * 150, bean_2.getSum());
	}

	@Test
	public void operationReturnsValueInCaseOfSingleMBeanInPool() throws Exception {
		MBeanWithOperationThatReturnsValue mbeanOpWithValue = new MBeanWithOperationThatReturnsValue();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(mbeanOpWithValue), defaultSettings(), false);

		assertEquals(15, (int) mbean.invoke("sum", new Object[]{7, 8}, new String[]{"int", "int"}));
	}

	@Test
	public void itShouldThrowExceptionForNonPublicOperations() {
		MBeanWithNonPublicOperation instance = new MBeanWithNonPublicOperation();
		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<MBeanWithNonPublicOperation> beans = List.of(instance);
		JmxBeanSettings settings = defaultSettings();
		try {
			dynamicMBeanFactory.createDynamicMBean(beans, settings, false);
			fail();
		} catch (IllegalStateException e) {
			assertThat(e.getMessage(), containsString( "A method \"action\" in class '" + MBeanWithNonPublicOperation.class.getName() +
					"' annotated with @JmxOperation should be declared public"));
		}
	}

	@Ignore("does not work concurrently yet")
	@Test
	public void operationReturnsNullInCaseOfSeveralMBeansInPool() throws Exception {
		MBeanWithOperationThatReturnsValue mbeanOpWithValue_1 = new MBeanWithOperationThatReturnsValue();
		MBeanWithOperationThatReturnsValue mbeanOpWithValue_2 = new MBeanWithOperationThatReturnsValue();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(mbeanOpWithValue_1, mbeanOpWithValue_2), defaultSettings(), false);

		assertNull(mbean.invoke("sum", new Object[]{7, 8}, new String[]{"int", "int"}));
	}

	@Test
	public void getOperationWithArgument() throws Exception {
		MBeanWithGetOperationWithArgument mbeanWithGet = new MBeanWithGetOperationWithArgument();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(mbeanWithGet), defaultSettings(), false);

		int argument = 123;
		int result = (int) mbean.invoke("getValue", new Object[]{argument}, new String[]{"int"});
		assertEquals(argument, result);
	}

	// helpers
	public static class BeanStubWithOperations implements ConcurrentJmxBean {
		private int count = 0;
		private String info = "";
		private long sum = 0;

		public int getCount() {
			return count;
		}

		public String getInfo() {
			return info;
		}

		public long getSum() {
			return sum;
		}

		@JmxOperation(name = "increment")
		public void inc() {
			count++;
		}

		@JmxOperation
		public void addInfo(@JmxParameter("information") String info) {
			this.info += info;
		}

		@JmxOperation
		public void multiplyAndAdd(long valueOne, long valueTwo) {
			sum += valueOne * valueTwo;
		}
	}

	public static final class MBeanWithOperationThatReturnsValue implements ConcurrentJmxBean {

		@SuppressWarnings("unused")
		@JmxOperation
		public int sum(int a, int b) {
			return a + b;
		}
	}

	public static class MBeanWithNonPublicOperation implements ConcurrentJmxBean {
		@SuppressWarnings("unused")
		@JmxOperation
		void action() {
		}
	}

	public static class MBeanWithGetOperationWithArgument implements ConcurrentJmxBean {
		@SuppressWarnings("unused")
		@JmxOperation
		public int getValue(int value) {
			return value;
		}
	}

	// custom matchers
	public static <T> Matcher<Map<T, ?>> hasKey(T key) {
		return new BaseMatcher<>() {

			@Override
			public void describeTo(Description description) {
				description.appendText("has key \"" + key.toString() + "\"");
			}

			@SuppressWarnings("unchecked")
			@Override
			public boolean matches(Object item) {
				if (item == null) {
					return false;
				}
				Map<T, ?> map = (Map<T, ?>) item;
				return map.containsKey(key);
			}
		};
	}

	public static Matcher<MBeanOperationInfo> hasParameter(String name, String type) {
		return new BaseMatcher<>() {
			@Override
			public boolean matches(Object item) {
				if (item == null) {
					return false;
				}
				MBeanOperationInfo operation = (MBeanOperationInfo) item;
				for (MBeanParameterInfo param : operation.getSignature()) {
					if (param.getName().equals(name) && param.getType().equals(type)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("has parameter with name \"" + name + "\" and type \"" + type + "\"");
			}
		};
	}

	public static Matcher<MBeanOperationInfo> hasReturnType(String type) {
		return new BaseMatcher<>() {
			@Override
			public boolean matches(Object item) {
				if (item == null) {
					return false;
				}
				MBeanOperationInfo operation = (MBeanOperationInfo) item;
				return operation.getReturnType().equals(type);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("has return type " + type);
			}
		};
	}

}
