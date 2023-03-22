package io.activej.jmx;

import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import org.junit.Test;

import javax.management.Attribute;
import javax.management.DynamicMBean;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class DynamicMBeanFactoryAttributesEnumTest {

	@Test
	public void properlyGetsEnum() throws Exception {
		MBeanWithEnum mBeanWithEnum = new MBeanWithEnum(TestEnum.A);
		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithEnum);

		assertEquals("A", mbean.getAttribute("testEnum"));
	}

	@Test
	public void properlySetsEnum() throws Exception {
		MBeanWithEnum mBeanWithEnum = new MBeanWithEnum(TestEnum.A);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithEnum);

		assertEquals("A", mbean.getAttribute("testEnum"));

		mbean.setAttribute(new Attribute("testEnum", "B"));

		assertEquals("B", mbean.getAttribute("testEnum"));
	}

	@Test
	public void properlyCallsEnumOperation() throws Exception {
		MBeanWithEnum mBeanWithEnum = new MBeanWithEnum(TestEnum.C);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithEnum);

		assertEquals("C", mbean.invoke("getTestEnumOp", null, null));
	}

	@Test
	public void properlyPassesEnumAsAnArgument() throws Exception {
		MBeanWithEnum mBeanWithEnum = new MBeanWithEnum(TestEnum.A);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithEnum);

		assertEquals("A", mbean.getAttribute("testEnum"));

		mbean.invoke("setTestEnumOp", new Object[]{"C"}, new String[]{String.class.getName()});

		assertEquals("C", mbean.getAttribute("testEnum"));
	}

	// region helper classes
	@JmxBean(JmxBeanAdapterStub.class)
	public static class MBeanWithEnum {
		private TestEnum testEnum;

		public MBeanWithEnum(TestEnum testEnum) {
			this.testEnum = testEnum;
		}

		@JmxAttribute
		public TestEnum getTestEnum() {
			return testEnum;
		}

		@JmxAttribute
		public void setTestEnum(TestEnum testEnum) {
			this.testEnum = testEnum;
		}

		@JmxOperation
		public TestEnum getTestEnumOp() {
			return testEnum;
		}

		@JmxOperation
		public void setTestEnumOp(TestEnum testEnum) {
			this.testEnum = testEnum;
		}
	}

	public enum TestEnum {
		A, B, C
	}
	// endregion

	// region helper methods
	public static DynamicMBean createDynamicMBeanFor(MBeanWithEnum mBean) {
		return DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(mBean), JmxBeanSettings.create(), false);
	}
	// endregion
}
