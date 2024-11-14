package io.activej.jmx;

import io.activej.inject.Key;
import io.activej.jmx.helper.MBeanServerStub;
import org.junit.Test;

import javax.management.MXBean;
import javax.management.ObjectName;
import java.util.Map;

import static io.activej.jmx.helper.CustomMatchers.objectName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class MXBeansRegistrationTest {

	private final MBeanServerStub mBeanServer = new MBeanServerStub();
	private final DynamicMBeanFactory mbeanFactory = DynamicMBeanFactory.create();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
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

	public interface ServiceStubMXBean {
		int getCount();
	}

	public static class ServiceStub implements ServiceStubMXBean {

		@Override
		public int getCount() {
			return 0;
		}
	}

	// region transitive implementations of MXBean interface through another interface
	@Test
	public void itShouldRegisterClassWhichImplementsMXBeanInterfaceTransitivelyThroughAnotherInterface() {
		ServiceTransitiveInterface service = new ServiceTransitiveInterface();

		Key<?> key = Key.of(ServiceTransitiveInterface.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceTransitiveInterface");
		assertSame(service, registeredMBeans.get(objectName));
	}

	public interface InterfaceMXBean {
		int getCount();
	}

	public interface TransitiveInterface extends InterfaceMXBean {

	}

	public static class ServiceTransitiveInterface implements TransitiveInterface {

		@Override
		public int getCount() {
			return 0;
		}
	}
	// endregion

	// region transitive implementation of MXBean interface through abstract class
	@Test
	public void itShouldRegisterClassWhichImplementsMXBeanInterfaceTransitivelyThroughAbstractClass() {
		ServiceTransitiveClass service = new ServiceTransitiveClass();

		Key<?> key = Key.of(ServiceTransitiveClass.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceTransitiveClass");
		assertSame(service, registeredMBeans.get(objectName));
	}

	public abstract static class TransitiveClass implements InterfaceMXBean {

	}

	public static class ServiceTransitiveClass extends TransitiveClass {

		@Override
		public int getCount() {
			return 0;
		}
	}
	// endregion

	// region registration of class that implements interface with @MXBean annotation
	@Test
	public void itShouldRegisterClassWhichImplementsInterfaceAnnotatedWithMXBean() {
		ServiceWithMXBeanInterfaceAnnotation service = new ServiceWithMXBeanInterfaceAnnotation();

		Key<?> key = Key.of(ServiceWithMXBeanInterfaceAnnotation.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceWithMXBeanInterfaceAnnotation");
		assertSame(service, registeredMBeans.get(objectName));
	}

	@MXBean
	public interface JMXInterfaceWithAnnotation {
		int getCount();
	}

	public static class ServiceWithMXBeanInterfaceAnnotation implements JMXInterfaceWithAnnotation {
		@Override
		public int getCount() {
			return 0;
		}
	}
	// endregion
}
