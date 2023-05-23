import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.time.Instant;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class PromiseAdvancedExample {
	private static final ExecutorService executor = newSingleThreadExecutor();

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
			.withCurrentThread()
			.build();

		firstExample();
		secondExample();

		eventloop.run();
		executor.shutdown();
	}

	public static void firstExample() {
		//[START REGION_1]
		Promise<Integer> firstNumber = Promise.of(10);
		Promise<Integer> secondNumber = Promises.delay(2000, 100);

		Promise<Integer> result = firstNumber.combine(secondNumber, Integer::sum);
		result.whenResult(res -> System.out.println("The first result is " + res));
		//[END REGION_1]
	}

	private static void secondExample() {
		//[START REGION_2]
		int someValue = 1000;
		int delay = 1000;     // in milliseconds
		int interval = 2000;  // also in milliseconds
		Promise<Integer> intervalPromise = Promises.interval(interval, Promise.of(someValue));
		Promise<Integer> schedulePromise = Promises.schedule(someValue * 2, Instant.now());
		Promise<Integer> delayPromise = Promises.delay(delay, someValue);

		Promise<Integer> result = intervalPromise
			.combine(schedulePromise, (first, second) -> first - second)
			.combine(delayPromise, Integer::sum);

		result.whenResult(res -> System.out.println("The second result is " + res));
		//[END REGION_2]
	}
}
