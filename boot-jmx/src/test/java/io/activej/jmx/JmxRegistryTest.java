package io.activej.jmx;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;
import io.activej.jmx.api.JmxBean;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import io.activej.jmx.helper.MBeanServerStub;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.activej.jmx.helper.CustomMatchers.objectName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxRegistryTest {
	private static final String DOMAIN = ServiceStub.class.getPackage().getName();

	private MBeanServerStub mBeanServer;
	private DynamicMBeanFactory mBeanFactory;
	private JmxRegistry jmxRegistry;

	@Before
	public void setUp() {
		mBeanServer = new MBeanServerStub();
		mBeanFactory = DynamicMBeanFactory.create();
		jmxRegistry = JmxRegistry.create(mBeanServer, mBeanFactory);
	}

	@Test
	public void unregisterSingletonInstance() {
		ServiceStub service = new ServiceStub();

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterSingleton(key, service);

		Set<ObjectName> unregisteredMBeans = mBeanServer.getUnregisteredMBeans();
		assertEquals(1, unregisteredMBeans.size());

		ObjectName objectName = objectName(DOMAIN + ":type=ServiceStub,annotation=BasicService");
		assertEquals(objectName, unregisteredMBeans.iterator().next());
	}

	@Test
	public void unregisterWorkers() {
		WorkerPool workerPool = getWorkerPool();
		WorkerPool.Instances<ServiceStub> instances = workerPool.getInstances(ServiceStub.class);

		ServiceStub worker_1 = instances.get(0);
		ServiceStub worker_2 = instances.get(1);
		ServiceStub worker_3 = instances.get(2);

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterWorkers(workerPool, key, List.of(worker_1, worker_2, worker_3));

		Set<ObjectName> unregisteredMBeans = mBeanServer.getUnregisteredMBeans();
		assertEquals(4, unregisteredMBeans.size());

		for (int i = 0; i < 2; i++) {
			ObjectName objectName = objectName(DOMAIN + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-" + i);
			assertTrue(unregisteredMBeans.contains(objectName));
		}

		ObjectName objectName = objectName(DOMAIN + ":type=ServiceStub,annotation=BasicService,scope=Worker");
		assertTrue(unregisteredMBeans.contains(objectName));
	}

	@Test
	public void registerWorkersWithPredicate() {
		jmxRegistry = JmxRegistry.builder(mBeanServer, mBeanFactory)
			.withWorkerPredicate(($, workerId) -> workerId == 0)
			.build();

		WorkerPool workerPool = getWorkerPool();
		WorkerPool.Instances<ServiceStub> instances = workerPool.getInstances(ServiceStub.class);

		ServiceStub worker_1 = instances.get(0);
		ServiceStub worker_2 = instances.get(1);
		ServiceStub worker_3 = instances.get(2);

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.registerWorkers(workerPool, key, List.of(worker_1, worker_2, worker_3), JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(2, registeredMBeans.size());

		ObjectName objectName = objectName(DOMAIN + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-0");
		assertTrue(registeredMBeans.containsKey(objectName));

		objectName = objectName(DOMAIN + ":type=ServiceStub,annotation=BasicService,scope=Worker");
		assertTrue(registeredMBeans.containsKey(objectName));
	}

	@Test
	public void conditionalRegisterSingletons() {
		String skipQualifier = "SKIP";

		jmxRegistry = JmxRegistry.builder(mBeanServer, mBeanFactory)
			.withObjectNameMapping(protoObjectName -> {
				if (skipQualifier.equals(protoObjectName.qualifier())) {
					return null;
				}
				return protoObjectName;
			})
			.build();

		ServiceStub registered = new ServiceStub();
		ServiceStub skipped = new ServiceStub();

		Key<?> registeredKey = Key.of(ServiceStub.class);
		jmxRegistry.registerSingleton(registeredKey, registered, JmxBeanSettings.create());

		Map<ObjectName, Object> registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());

		ObjectName objectName = objectName(DOMAIN + ":type=ServiceStub");
		assertTrue(registeredMBeans.containsKey(objectName));

		Key<?> skippedKey = Key.of(ServiceStub.class, skipQualifier);
		jmxRegistry.registerSingleton(skippedKey, skipped, JmxBeanSettings.create());

		registeredMBeans = mBeanServer.getRegisteredMBeans();
		assertEquals(1, registeredMBeans.size());
		assertTrue(registeredMBeans.containsKey(objectName));
	}

	private WorkerPool getWorkerPool() {
		Injector injector = Injector.of(WorkerPoolModule.create(), new AbstractModule() {
			@Override
			protected void configure() {
				bind(ServiceStub.class).in(Worker.class).to(ServiceStub::new);
			}
		});
		WorkerPools pools = injector.getInstance(WorkerPools.class);
		return pools.createPool(3);
	}

	// annotations
	@Retention(RetentionPolicy.RUNTIME)
	@QualifierAnnotation
	public @interface BasicService {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@QualifierAnnotation
	public @interface Group {
		String value() default "";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@QualifierAnnotation
	public @interface ComplexAnnotation {
		int threadId() default -1;

		String threadName() default "defaultThread";
	}

	// helpers for creating annotations
	public static BasicService createBasicServiceAnnotation() {
		return new BasicService() {
			@Override
			public Class<? extends Annotation> annotationType() {
				return BasicService.class;
			}
		};
	}

	public static Group createGroupAnnotation(String value) {
		return new Group() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return Group.class;
			}

			@Override
			public String value() {
				return value;
			}
		};
	}

	public static ComplexAnnotation createComplexAnnotation(int threadId, String threadName) {
		return new ComplexAnnotation() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return ComplexAnnotation.class;
			}

			@Override
			public int threadId() {
				return threadId;
			}

			@Override
			public String threadName() {
				return threadName;
			}
		};
	}

	// helper class
	@JmxBean(JmxBeanAdapterStub.class)
	public static final class ServiceStub {

	}
}
