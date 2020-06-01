import io.activej.di.Injector;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.worker.*;

//[START EXAMPLE]
public final class WorkerPoolModuleExample extends AbstractModule {
	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(4);
	}

	@Provides
	@Worker
	String string(@WorkerId int workerId) {
		return "Hello from worker #" + workerId;
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(WorkerPoolModule.create(), new WorkerPoolModuleExample());
		WorkerPool workerPool = injector.getInstance(WorkerPool.class);
		WorkerPool.Instances<String> strings = workerPool.getInstances(String.class);
		strings.forEach(System.out::println);
	}
}
//[END EXAMPLE]
