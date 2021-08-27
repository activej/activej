import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

//[START EXAMPLE]
@SuppressWarnings("Convert2MethodRef")
public class PromiseChainExample {
	private static final Eventloop eventloop = Eventloop.create().withCurrentThread();

	public static void main(String[] args) {
		//[START REGION_1]
		doSomeProcess()
				.whenResult(result -> System.out.printf("Result of some process is '%s'%n", result))
				.whenException(e -> System.out.printf("Exception after some process is '%s'%n", e.getMessage()))
				.map(s -> s.toLowerCase())
				.map((result, e) -> e == null ? String.format("The mapped result is '%s'", result) : e.getMessage())
				.whenResult(s -> System.out.println(s));
		//[END REGION_1]
		Promise.complete()
				.then(PromiseChainExample::loadData)
				.whenResult(result -> System.out.printf("Loaded data is '%s'%n", result));
		eventloop.run();
	}

	private static Promise<String> loadData() {
		return Promise.of("Hello World");
	}

	public static Promise<String> doSomeProcess() {
		return Promises.delay(1000, Math.random() > 0.5 ?
				Promise.of("Hello World") :
				Promise.ofException(new RuntimeException("Something went wrong")));
	}
}
//[END EXAMPLE]
