import io.activej.eventloop.Eventloop;

import static java.lang.System.currentTimeMillis;

//[START EXAMPLE]
public final class EventloopExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
				.withCurrentThread()
				.build();
		long startTime = currentTimeMillis();

		// #2
		eventloop.delay(3000L, () -> System.out.println("Eventloop.delay(3000) is finished, time: " + (currentTimeMillis() - startTime)));
		eventloop.delay(1000L, () -> System.out.println("Eventloop.delay(1000) is finished, time: " + (currentTimeMillis() - startTime)));
		eventloop.delay(100L, () -> System.out.println("Eventloop.delay(100) is finished, time: " + (currentTimeMillis() - startTime)));

		// #1
		System.out.println("Before running eventloop, time: " + (currentTimeMillis() - startTime));

		eventloop.run();
	}
}
//[END EXAMPLE]
