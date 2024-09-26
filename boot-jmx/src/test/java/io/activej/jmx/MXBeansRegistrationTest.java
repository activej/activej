package io.activej.jmx;

import io.activej.inject.Key;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.*;

import static io.activej.jmx.helper.CustomMatchers.objectname;

public class MXBeansRegistrationTest {

	@Rule
	public final JUnitRuleMockery context = new JUnitRuleMockery();

	private final MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private final DynamicMBeanFactory mbeanFactory = DynamicMBeanFactory.create();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void itShouldRegisterStandardMBeans() throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(with(service), with(objectname(domain + ":type=ServiceStub")));
		}});

		Key<?> key = Key.of(ServiceStub.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());
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
	public void itShouldRegisterClassWhichImplementsMXBeanInterfaceTransitivelyThroughAnotherInterface()
		throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {

		ServiceTransitiveInterface service = new ServiceTransitiveInterface();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(
				with(service), with(objectname(domain + ":type=ServiceTransitiveInterface"))
			);
		}});

		Key<?> key = Key.of(ServiceTransitiveInterface.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());
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
	public void itShouldRegisterClassWhichImplementsMXBeanInterfaceTransitivelyThroughAbstractClass()
		throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {

		ServiceTransitiveClass service = new ServiceTransitiveClass();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(
				with(service), with(objectname(domain + ":type=ServiceTransitiveClass"))
			);
		}});

		Key<?> key = Key.of(ServiceTransitiveClass.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());
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
	public void itShouldRegisterClassWhichImplementsInterfaceAnnotatedWithMXBean()
		throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {

		ServiceWithMXBeanInterfaceAnnotation service = new ServiceWithMXBeanInterfaceAnnotation();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).registerMBean(
				with(service), with(objectname(domain + ":type=ServiceWithMXBeanInterfaceAnnotation"))
			);
		}});

		Key<?> key = Key.of(ServiceWithMXBeanInterfaceAnnotation.class);
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());
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
