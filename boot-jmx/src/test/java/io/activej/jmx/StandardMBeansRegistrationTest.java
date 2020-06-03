package io.activej.jmx;

import io.activej.di.Key;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.MBeanServer;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static io.activej.jmx.helper.CustomMatchers.objectname;

public class StandardMBeansRegistrationTest {

	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();

	private final MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, DynamicMBeanFactory.create());
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void itShouldRegisterStandardMBeans() throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key = Key.of(ServiceStub.class);
		jmxRegistry.registerSingleton(key, service, defaultSettings());
	}

	@Test
	public void itShouldNotRegisterClassesThatAreNotMBeans() {
		NonMBeanServiceImpl nonMBean = new NonMBeanServiceImpl();

		context.checking(new Expectations() {
			// we do not expect any calls
			// any call of mBeanServer will produce error
		});

		Key<?> key = Key.of(NonMBeanServiceImpl.class);
		jmxRegistry.registerSingleton(key, nonMBean, defaultSettings());
	}

	public interface ServiceStubMBean {
		int getCount();
	}

	public static class ServiceStub implements ServiceStubMBean {

		@Override
		public int getCount() {
			return 0;
		}
	}

	public interface NonMBeanService {
		int getTotal();
	}

	public static class NonMBeanServiceImpl implements NonMBeanService {

		@Override
		public int getTotal() {
			return 0;
		}
	}
}
