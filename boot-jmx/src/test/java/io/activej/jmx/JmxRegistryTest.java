package io.activej.jmx;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.QualifierAnnotation;
import io.activej.inject.module.AbstractModule;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.worker.WorkerPool;
import io.activej.worker.WorkerPoolModule;
import io.activej.worker.WorkerPools;
import io.activej.worker.annotation.Worker;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import javax.management.MBeanServer;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.List;

import static io.activej.jmx.helper.CustomMatchers.objectname;

public class JmxRegistryTest {
	@Rule
	public final JUnitRuleMockery context = new JUnitRuleMockery();

	private final MBeanServer mBeanServer = context.mock(MBeanServer.class);
	private final DynamicMBeanFactory mbeanFactory = DynamicMBeanFactory.create();
	private final JmxRegistry jmxRegistry = JmxRegistry.create(mBeanServer, mbeanFactory);
	private final String domain = ServiceStub.class.getPackage().getName();

	@Test
	public void unregisterSingletonInstance() throws Exception {
		ServiceStub service = new ServiceStub();

		context.checking(new Expectations() {{
			oneOf(mBeanServer).unregisterMBean(with(objectname(domain + ":type=ServiceStub,annotation=BasicService")));
		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterSingleton(key, service);
	}

	@Test
	public void unregisterWorkers() throws Exception {
		WorkerPool workerPool = getWorkerPool();
		WorkerPool.Instances<ServiceStub> instances = workerPool.getInstances(ServiceStub.class);

		ServiceStub worker_1 = instances.get(0);
		ServiceStub worker_2 = instances.get(1);
		ServiceStub worker_3 = instances.get(2);

		context.checking(new Expectations() {{
			// checking calls and names for each worker separately
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-0")));
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-1")));
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker,workerId=worker-2")));

			// checking calls and names for worker_pool DynamicMBean
			oneOf(mBeanServer).unregisterMBean(
					with(objectname(domain + ":type=ServiceStub,annotation=BasicService,scope=Worker")));

		}});

		BasicService basicServiceAnnotation = createBasicServiceAnnotation();
		Key<?> key = Key.of(ServiceStub.class, basicServiceAnnotation);
		jmxRegistry.unregisterWorkers(workerPool, key, List.of(worker_1, worker_2, worker_3));
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

	// helper classes
	public static final class ServiceStub implements ConcurrentJmxBean {

	}
}
