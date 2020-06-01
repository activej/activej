import io.activej.di.Injector;
import io.activej.di.Key;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.eventloop.Eventloop;
import io.activej.worker.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

//[START EXAMPLE]
public final class MultithreadedWorkerCollab extends AbstractModule {

	@Provides
	@Worker
	Eventloop eventloop(@WorkerId int wid, ConcurrentLinkedQueue<Integer> queue) {
		Eventloop eventloop = Eventloop.create();
		eventloop.delay(100 * wid, () -> queue.add(wid));
		return eventloop;
	}

	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(25);
	}

	@Provides
	ConcurrentLinkedQueue<Integer> queue() {
		return new ConcurrentLinkedQueue<>();
	}

	public static void main(String[] args) throws InterruptedException {
		Injector injector = Injector.of(WorkerPoolModule.create(), new MultithreadedWorkerCollab());
		WorkerPool workerPool = injector.getInstance(WorkerPool.class);
		WorkerPool.Instances<Eventloop> eventloops = workerPool.getInstances(Eventloop.class);

		List<Thread> threads = new ArrayList<>();
		for (Eventloop eventloop : eventloops.getList()) {
			Thread thread = new Thread(eventloop);
			threads.add(thread);
		}

		Collections.shuffle(threads);
		threads.forEach(Thread::start);

		for (Thread thread : threads) {
			thread.join();
		}

		ConcurrentLinkedQueue<Integer> queue = injector.getInstance(new Key<ConcurrentLinkedQueue<Integer>>() {});

		while (!queue.isEmpty()) {
			System.out.println(queue.poll());
		}

	}
}
//[END EXAMPLE]
