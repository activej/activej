package io.activej.jmx;

import io.activej.inject.Key;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.helper.MBeanServerStub;
import org.junit.Test;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;
import java.util.Map;

import static io.activej.jmx.helper.CustomMatchers.objectName;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class DynamicMBeanRegistrationTest {

	private final MBeanServerStub mBeanServer = new MBeanServerStub();
	private final DynamicMBeanFactory mbeanFactory = DynamicMBeanFactory.create();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = CustomKeyClass.class.getPackage().getName();

	@Test
	public void itShouldRegisterDynamicMBeans() throws Exception {
		DynamicMBeanStub service = new DynamicMBeanStub();

		Key<?> key = Key.of(CustomKeyClass.class, createCustomAnnotation("Global"));
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=CustomKeyClass,CustomAnnotation=Global");
		assertSame(service, registeredMBeans.get(objectName));
	}

	@Test
	public void itShouldRegisterDynamicMBeansWithOverriddenAttributes() throws Exception {
		PublicMBeanSubclass instance = new PublicMBeanSubclass();
		DynamicMBean mBean = DynamicMBeanFactory.create()
			.createDynamicMBean(List.of(instance), JmxBeanSettings.create(), false);

		assertEquals(instance.getValue(), mBean.getAttribute("value"));
	}

	@Test
	public void itShouldThrowExceptionForNonPublicMBeans() {
		NonPublicMBean instance = new NonPublicMBean();

		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<NonPublicMBean> beans = List.of(instance);
		JmxBeanSettings settings = JmxBeanSettings.create();
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> dynamicMBeanFactory.createDynamicMBean(beans, settings, false));
		assertThat(e.getMessage(), containsString(
			"A class '" + NonPublicMBean.class.getName() +
			"' containing methods annotated with @JmxAttribute should be declared public"));
	}

	@Test
	public void itShouldNotThrowExceptionForNonPublicMBeansWithNoJmxFields() throws Exception {
		NonPublicMBeanSubclass instance = new NonPublicMBeanSubclass();
		DynamicMBean mBean = DynamicMBeanFactory.create()
			.createDynamicMBean(List.of(instance), JmxBeanSettings.create(), false);

		assertEquals(instance.getValue(), mBean.getAttribute("value"));
	}

	// region helper classes
	public static class DynamicMBeanStub implements DynamicMBean {

		@Override
		public Object getAttribute(String attribute) {
			return null;
		}

		@Override
		public void setAttribute(Attribute attribute) {

		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			return null;
		}

		@Override
		public AttributeList setAttributes(AttributeList attributes) {
			return null;
		}

		@Override
		public Object invoke(String actionName, Object[] params, String[] signature) {
			return null;
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return null;
		}
	}

	static class NonPublicMBean implements ConcurrentJmxBean {
		@JmxAttribute
		public int getValue() {
			return 123;
		}
	}

	public static class PublicMBean implements ConcurrentJmxBean {
		@JmxAttribute
		public int getValue() {
			return 123;
		}
	}

	static class NonPublicMBeanSubclass extends PublicMBean {
	}

	public static class PublicMBeanSubclass extends PublicMBean {
		@JmxAttribute
		public int getValue() {
			return 321;
		}
	}

	public static class CustomKeyClass {

	}

	@Retention(RetentionPolicy.RUNTIME)
	@QualifierAnnotation
	public @interface CustomAnnotation {
		String value() default "";
	}

	public static CustomAnnotation createCustomAnnotation(String value) {
		return new CustomAnnotation() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return CustomAnnotation.class;
			}

			@Override
			public String value() {
				return value;
			}
		};
	}
	// endregion
}
