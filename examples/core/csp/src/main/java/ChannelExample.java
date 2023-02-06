import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;

import java.util.List;
import java.util.stream.Collectors;

public final class ChannelExample {
	//[START REGION_1]
	private static void supplierOfValues() {
		ChannelSuppliers.ofValues("1", "2", "3", "4", "5")
				.streamTo(ChannelConsumers.ofConsumer(System.out::println));
	}

	private static void supplierOfList(List<String> list) {
		ChannelSuppliers.ofList(list)
				.streamTo(ChannelConsumers.ofConsumer(System.out::println));
	}

	private static void map() {
		ChannelSuppliers.ofValues(1, 2, 3, 4, 5)
				.map(integer -> integer + " times 10 = " + integer * 10)
				.streamTo(ChannelConsumers.ofConsumer(System.out::println));
	}

	private static void toCollector() {
		ChannelSuppliers.ofValues(1, 2, 3, 4, 5)
				.toCollector(Collectors.toList())
				.whenResult(x -> System.out.println(x));
	}

	private static void filter() {
		ChannelSuppliers.ofValues(1, 2, 3, 4, 5, 6)
				.filter(integer -> integer % 2 == 0)
				.streamTo(ChannelConsumers.ofConsumer(System.out::println));
	}
	//[END REGION_1]

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
				.withCurrentThread()
				.build();
		supplierOfValues();
		supplierOfList(List.of("One", "Two", "Three"));
		map();
		toCollector();
		filter();
		eventloop.run();
	}
}
