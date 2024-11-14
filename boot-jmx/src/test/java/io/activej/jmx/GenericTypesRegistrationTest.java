package io.activej.jmx;

import io.activej.inject.Key;
import io.activej.jmx.helper.MBeanServerStub;
import org.junit.Test;

import javax.management.ObjectName;
import java.util.List;
import java.util.Map;

import static io.activej.jmx.helper.CustomMatchers.objectName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class GenericTypesRegistrationTest {

	private final MBeanServerStub mBeanServer = new MBeanServerStub();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, DynamicMBeanFactory.create());
	private final String domain = ServiceStubOne.class.getPackage().getName();

	@Test
	public void itShouldFormProperNameForTypeWithSingleGenericParameter() throws Exception {
		ServiceStubOne<?> service = new ServiceStubOne<>();

		Key<?> key = new Key<ServiceStubOne<String>>() {};
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceStubOne,T1=String");
		assertSame(service, registeredMBeans.get(objectName));
	}

	@Test
	public void itShouldFormProperNameForTypeWithSeveralGenericParameter() throws Exception {
		ServiceStubThree<String, Integer, Long> service = new ServiceStubThree<>();

		Key<?> key = new Key<ServiceStubThree<String, Integer, Long>>() {};
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceStubThree,T1=String,T2=Integer,T3=Long");
		assertSame(service, registeredMBeans.get(objectName));
	}

	@Test
	public void itShouldFormProperNameForTypeWithNestedGenerics() throws Exception {
		ServiceStubThree<String, List<Integer>, Map<Long, List<String>>> service = new ServiceStubThree<>();

		Key<?> key = new Key<ServiceStubThree<String, List<Integer>, Map<Long, List<String>>>>() {};
		jmxRegistry.registerSingleton(key, service, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(domain + ":type=ServiceStubThree,T1=String,T2=List<Integer>,T3=Map<Long;List<String>>");
		assertSame(service, registeredMBeans.get(objectName));
	}

	public interface ServiceStubOneMBean {
		int getCount();
	}

	public static class ServiceStubOne<T> implements ServiceStubOneMBean {
		private T value;

		@Override
		public int getCount() {
			return 0;
		}
	}

	public interface ServiceStubThreeMBean {
		int getCount();
	}

	public static class ServiceStubThree<A, B, C> implements ServiceStubThreeMBean {
		private A a;
		private B b;
		private C c;

		@Override
		public int getCount() {
			return 0;
		}
	}
}
