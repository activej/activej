package io.activej.common.concurrent;

import io.activej.common.ref.RefInt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A stress test for the {@link ObjectPool} class.
 * This class is used to evaluate the performance of an object pool under high load by simulating multiple threads
 * concurrently borrowing and returning objects to the pool.
 */
public class ObjectPoolStressTest {
	private static final byte[] SINGLE_BYTE_ARRAY = new byte[1];
	public static final int TOTAL_ROUNDS = 100;
	public static final int TOTAL_THREADS = 100;
	public static final int TOTAL_ITERATIONS = 100_000;

	/**
	 * Main method to run the stress test.
	 *
	 * @param args Command line arguments (not used).
	 * @throws InterruptedException If the thread execution is interrupted.
	 */
	public static void main(String[] args) throws InterruptedException {
		executeStressTest();
	}

	/**
	 * Executes multiple rounds of stress tests on the object pool.
	 * Each round spawns multiple threads that repeatedly borrow and return objects to the pool.
	 *
	 * @throws InterruptedException If the thread execution is interrupted.
	 */
	private static void executeStressTest() throws InterruptedException {
		long startTime = System.currentTimeMillis();
		RefInt allocationCounter = new RefInt(0);
		ObjectPool<byte[]> objectPool = new ObjectPool<>(128); // Set fixed capacity to 128

		for (int currentRound = 0; currentRound < TOTAL_ROUNDS; currentRound++) {
			executeSingleRound(objectPool, allocationCounter);
			int totalOperations = (currentRound + 1) * TOTAL_THREADS * TOTAL_ITERATIONS;
			long elapsedTime = System.currentTimeMillis() - startTime;
			int opsPerMs = (int) (totalOperations / elapsedTime);
			System.out.println("Round: " + (currentRound + 1));
			System.out.println("Total Operations: " + totalOperations);
			System.out.println("Total Allocations: " + allocationCounter.get());
			System.out.println("Pool Size: " + objectPool.size());
			System.out.println("Pool Capacity: " + objectPool.capacity());
			System.out.println("Operations per Millisecond: " + opsPerMs + " ");
			System.out.println();
		}
	}

	/**
	 * Executes a single round of the stress test by using a fixed thread pool.
	 *
	 * @param objectPool The object pool to be tested.
	 * @param allocationCounter A reference counter for counting the number of allocations made.
	 * @throws InterruptedException If the thread execution is interrupted.
	 */
	private static void executeSingleRound(ObjectPool<byte[]> objectPool, RefInt allocationCounter) throws InterruptedException {
		ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_THREADS);

		for (int threadIndex = 0; threadIndex < TOTAL_THREADS; threadIndex++) {
			executorService.execute(() -> {
				for (int iteration = 0; iteration < TOTAL_ITERATIONS; iteration++) {
					byte[] byteArray = objectPool.poll();
					if (byteArray == null) {
						byteArray = new byte[1];
						allocationCounter.inc();
					}
					objectPool.offer(byteArray);
				}
			});
		}

		executorService.shutdown();
		if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
			System.err.println("Executor did not terminate in the specified time.");
			executorService.shutdownNow();
		}
	}
}

/**
 * A simple object pool class to manage the reuse of objects.
 *
 * @param <T> The type of objects managed by the pool.
 */
class ObjectPool<T> {
	private final List<T> pool;
	private final int capacity;

	/**
	 * Creates an object pool with a specified capacity.
	 *
	 * @param capacity The maximum number of objects the pool can hold.
	 */
	public ObjectPool(int capacity) {
		this.capacity = capacity;
		this.pool = new ArrayList<>(capacity);
	}

	/**
	 * Retrieves an object from the pool, or returns {@code null} if the pool is empty.
	 *
	 * @return an object from the pool or {@code null} if none are available.
	 */
	public synchronized T poll() {
		return pool.isEmpty() ? null : pool.remove(pool.size() - 1);
	}

	/**
	 * Returns an object to the pool.
	 *
	 * @param obj The object to be returned to the pool.
	 */
	public synchronized void offer(T obj) {
		if (obj != null && pool.size() < capacity) {
			pool.add(obj);
		}
	}

	/**
	 * Returns the current size of the pool.
	 *
	 * @return the number of objects currently in the pool.
	 */
	public synchronized int size() {
		return pool.size();
	}

	/**
	 * Returns the current capacity of the pool.
	 *
	 * @return the capacity of the pool.
	 */
	public synchronized int capacity() {
		return capacity;
	}
}
