import io.activej.eventloop.Eventloop;

// [START EXAMPLE]
public final class BasicExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create();

		eventloop.post(() -> System.out.println("Hello World"));

		eventloop.run();
	}
}
// [END EXAMPLE]
