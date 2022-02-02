import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;

import java.util.List;
import java.util.stream.Collectors;

public final class ChannelExample {
	//[START REGION_1]
	private static void supplierOfValues() {
		ChannelSupplier.of("1", "2", "3", "4", "5")
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	private static void supplierOfList(List<String> list) {
		ChannelSupplier.ofList(list)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	private static void map() {
		ChannelSupplier.of(1, 2, 3, 4, 5)
				.map(integer -> integer + " times 10 = " + integer * 10)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	private static void toCollector() {
		ChannelSupplier.of(1, 2, 3, 4, 5)
				.toCollector(Collectors.toList())
				.whenResult(x -> System.out.println(x));
	}

	private static void filter() {
		ChannelSupplier.of(1, 2, 3, 4, 5, 6)
				.filter(integer -> integer % 2 == 0)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}
	//[END REGION_1]

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		supplierOfValues();
		supplierOfList(List.of("One", "Two", "Three"));
		map();
		toCollector();
		filter();
		eventloop.run();
	}
}
