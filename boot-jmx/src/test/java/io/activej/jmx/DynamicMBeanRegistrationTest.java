package io.activej.jmx;

import io.activej.inject.Key;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static io.activej.jmx.helper.CustomMatchers.objectname;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DynamicMBeanRegistrationTest {

	@Rule
	public final JUnitRuleMockery context = new JUnitRuleMockery();

	private final MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private final DynamicMBeanFactory mbeanFactory = DynamicMBeanFactory.create();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = CustomKeyClass.class.getPackage().getName();

	@Test
	public void itShouldRegisterDynamicMBeans() throws Exception {
		DynamicMBeanStub service = new DynamicMBeanStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service),
					with(objectname(domain + ":type=CustomKeyClass,CustomAnnotation=Global"))
			);
		}});

		Key<?> key = Key.of(CustomKeyClass.class, createCustomAnnotation("Global"));
		jmxRegistry.registerSingleton(key, service, defaultSettings());
	}

	@Test
	public void itShouldRegisterDynamicMBeansWithOverriddenAttributes() throws Exception {
		PublicMBeanSubclass instance = new PublicMBeanSubclass();
		DynamicMBean mBean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(instance), defaultSettings(), false);

		assertEquals(instance.getValue(), mBean.getAttribute("value"));
	}

	@Test
	public void itShouldThrowExceptionForNonPublicMBeans() {
		NonPublicMBean instance = new NonPublicMBean();

		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<NonPublicMBean> beans = List.of(instance);
		JmxBeanSettings settings = defaultSettings();
		try {
			dynamicMBeanFactory.createDynamicMBean(beans, settings, false);
			fail();
		} catch (IllegalStateException e) {
			assertThat(e.getMessage(), containsString("A class '" + NonPublicMBean.class.getName() +
					"' containing methods annotated with @JmxAttribute should be declared public"));
		}
	}

	@Test
	public void itShouldNotThrowExceptionForNonPublicMBeansWithNoJmxFields() throws Exception {
		NonPublicMBeanSubclass instance = new NonPublicMBeanSubclass();
		DynamicMBean mBean = DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(instance), defaultSettings(), false);

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
