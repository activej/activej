package io.activej.jmx;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.jmx.GlobalSingletonsRegistrationTest.GlobalSingletonClass1.CustomClass;
import io.activej.launcher.LauncherService;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GlobalSingletonsRegistrationTest {
	private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

	@Test
	public void testMXBeanSingletonRegistration() throws Exception {
		int jmxValue = 55;
		GlobalSingletonClass2 globalSingletonClass2 = GlobalSingletonClass2.getInstance();
		globalSingletonClass2.setValue(jmxValue);

		Injector injector = Injector.of(
			JmxModule.builder()
				.withGlobalSingletons(globalSingletonClass2)
				.build()
		);
		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}
		ObjectName found;
		try {
			ObjectName objectName = server.queryNames(new ObjectName("*:type=" + GlobalSingletonClass2.class.getSimpleName() + ",*"), null).iterator().next();
			assertEquals(jmxValue, server.getAttribute(objectName, "Value"));
			found = objectName;
		} finally {
			unregisterMBeans();
		}
		assertNotNull(found);
	}

	@Test
	public void testMBeanSingletonRegistration() throws Exception {
		GlobalSingletonClass1 globalSingletonClass1 = GlobalSingletonClass1.getInstance();

		Injector injector = Injector.of(
			JmxModule.builder()
				.withGlobalSingletons(globalSingletonClass1)
				.build()
		);
		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}
		ObjectName found;
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		try {
			ObjectName objectName = server.queryNames(new ObjectName("*:type=" + GlobalSingletonClass1.class.getSimpleName() + ",*"), null).iterator().next();
			assertEquals(globalSingletonClass1.custom, server.getAttribute(objectName, "CustomClass"));
			found = objectName;
		} finally {
			unregisterMBeans();
		}
		assertNotNull(found);
	}

	private void unregisterMBeans() throws Exception {
		server.unregisterMBean(new ObjectName("io.activej.bytebuf:type=ByteBufPoolStats"));
		server.unregisterMBean(new ObjectName("io.activej.jmx:type=JmxRegistry"));
	}

	// region singletons
	public interface GlobalSingletonClass1MBean {
		CustomClass getCustomClass();
	}

	static class GlobalSingletonClass1 implements GlobalSingletonClass1MBean {
		private static final GlobalSingletonClass1 INSTANCE = new GlobalSingletonClass1();
		private final CustomClass custom = new CustomClass();

		private GlobalSingletonClass1() {}

		public static GlobalSingletonClass1 getInstance() {
			return INSTANCE;
		}

		@Override
		public CustomClass getCustomClass() {
			return custom;
		}

		static class CustomClass {}
	}

	public interface StubMXBean {
		int getValue();
	}

	public static class GlobalSingletonClass2 implements StubMXBean {
		private static final GlobalSingletonClass2 INSTANCE = new GlobalSingletonClass2();
		private int value;

		private GlobalSingletonClass2() {}

		public static GlobalSingletonClass2 getInstance() {
			return INSTANCE;
		}

		@Override
		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}
	}
	// endregion
}
