import io.activej.datastream.StreamSupplier;
import io.activej.datastream.ToListStreamConsumer;
import io.activej.datastream.processor.StreamFilter;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.processor.StreamUnion;
import io.activej.eventloop.Eventloop;

import java.util.function.ToIntFunction;

import static io.activej.common.exception.FatalErrorHandler.rethrow;

/**
 * Example of some simple builtin stream nodes.
 */
public final class BuiltinNodesExample {
	//[START REGION_1]
	private static void filter() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		StreamFilter<Integer, Integer> filter = StreamFilter.create(input -> input % 2 == 1);

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		supplier.transformWith(filter).streamTo(consumer);

		consumer.getResult().whenResult(v -> System.out.println(v));
	}
	//[END REGION_1]

	//[START REGION_2]
	private static void splitter() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		ToIntFunction<Object> hashSharder = item -> (item.hashCode() & Integer.MAX_VALUE) % 3;
		//creating a sharder of three parts for three consumers
		StreamSplitter<Integer, Integer> sharder = StreamSplitter.create(
				(item, acceptors) -> acceptors[hashSharder.applyAsInt(item)].accept(item));

		ToListStreamConsumer<Integer> first = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> second = ToListStreamConsumer.create();
		ToListStreamConsumer<Integer> third = ToListStreamConsumer.create();

		sharder.newOutput().streamTo(first);
		sharder.newOutput().streamTo(second);
		sharder.newOutput().streamTo(third);

		supplier.streamTo(sharder.getInput());

		first.getResult().whenResult(x -> System.out.println("first: " + x));
		second.getResult().whenResult(x -> System.out.println("second: " + x));
		third.getResult().whenResult(x -> System.out.println("third: " + x));
	}
	//[END REGION_2]

	//[START REGION_3]
	private static void mapper() {
		//creating a supplier of 10 numbers
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		//creating a mapper for the numbers
		StreamFilter<Integer, String> simpleMap = StreamFilter.mapper(x -> x + " times ten = " + x * 10);

		//creating a consumer which converts received values to list
		ToListStreamConsumer<String> consumer = ToListStreamConsumer.create();

		//applying the mapper to supplier and streaming the result to consumer
		supplier.transformWith(simpleMap).streamTo(consumer);

		//when consumer completes receiving values, the result is printed out
		consumer.getResult().whenResult(v -> System.out.println(v));
	}
	//[END REGION_3]

	//[START REGION_4]
	private static void union() {
		//creating three suppliers of numbers
		StreamSupplier<Integer> source0 = StreamSupplier.of(1, 2);
		StreamSupplier<Integer> source1 = StreamSupplier.of();
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 5);

		//creating a unifying transformer
		StreamUnion<Integer> streamUnion = StreamUnion.create();

		//creating a consumer which converts received values to list
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		//stream the sources into new inputs of the unifier
		source0.streamTo(streamUnion.newInput());
		source1.streamTo(streamUnion.newInput());
		source2.streamTo(streamUnion.newInput());

		//and stream the output of the unifier into the consumer
		streamUnion.getOutput().streamTo(consumer);

		//when consumer completes receiving values, the result is printed out
		consumer.getResult().whenResult(v -> System.out.println(v));
	}
	//[END REGION_4]

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
				.withCurrentThread()
				.withFatalErrorHandler(rethrow())
				.build();

		filter();
		splitter();
		mapper();
		union();

		eventloop.run();
	}
}
