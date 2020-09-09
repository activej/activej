import io.activej.async.function.AsyncConsumer;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelSplitter;
import io.activej.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//[START EXAMPLE]
public class SplitterExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		List<Integer> integers = Stream.iterate(1, i -> i + 1)
				.limit(5)
				.collect(Collectors.toList());

		ChannelSplitter<Integer> splitter = ChannelSplitter.create(ChannelSupplier.ofList(integers));

		List<Integer> list1 = new ArrayList<>();
		List<Integer> list2 = new ArrayList<>();
		List<Integer> list3 = new ArrayList<>();

		splitter.addOutput().set(ChannelConsumer.of(AsyncConsumer.of(list1::add)));
		splitter.addOutput().set(ChannelConsumer.of(AsyncConsumer.of(list2::add)));
		splitter.addOutput().set(ChannelConsumer.of(AsyncConsumer.of(list3::add)));

		eventloop.run();

		System.out.println("First list: " + list1);
		System.out.println("Second list: " + list2);
		System.out.println("Third list: " + list3);
	}
}
//[END EXAMPLE]
