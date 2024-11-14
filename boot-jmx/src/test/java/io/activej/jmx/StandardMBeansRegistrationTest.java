package io.activej.jmx;

import io.activej.inject.Key;
import io.activej.jmx.helper.MBeanServerStub;
import org.junit.Test;

import javax.management.ObjectName;
import java.util.Map;

import static io.activej.jmx.helper.CustomMatchers.objectName;
import static org.junit.Assert.*;

public class StandardMBeansRegistrationTest {

	private final MBeanServerStub mBeanServer = new MBeanServerStub();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, DynamicMBeanFactory.create());
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void itShouldRegisterStandardMBeans() {
		ServiceStub service = new ServiceStub();

		Key<?> key = Key.of(ServiceStub.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceStub");
		assertSame(service, registeredMBeans.get(objectName));
	}

	@Test
	public void itShouldNotRegisterClassesThatAreNotMBeans() {
		NonMBeanServiceImpl nonMBean = new NonMBeanServiceImpl();

		Key<?> key = Key.of(NonMBeanServiceImpl.class);
		jmxRegistry.registerSingleton(key, nonMBean, JmxBeanSettings.create());

		assertTrue(mBeanServer.getRegisteredMBeans().isEmpty());
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
