import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.local.CrdtStorage_Fs;
import io.activej.crdt.util.BinarySerializer_CrdtData;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.fs.Fs_Local;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.activej.serializer.BinarySerializers.*;

public final class CrdtFsConsolidationExample {

	private static Set<Integer> union(Set<Integer> first, Set<Integer> second) {
		Set<Integer> res = new HashSet<>();
		res.addAll(first);
		res.addAll(second);
		return res;
	}

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		ExecutorService executor = Executors.newSingleThreadExecutor();

		//[START REGION_1]
		// create our storage dir and an FS client which operates on that dir
		Path storage = Files.createTempDirectory("storage");
		Fs_Local fsClient = Fs_Local.create(eventloop, executor, storage);

		// our item is a set of integers, so we create a CRDT function for it
		// also each CRDT item needs to have a timestamp, so we wrap the sets
		// and the function using the TimestampContainer
		CrdtFunction<Set<Integer>> crdtFunction = CrdtFunction.ignoringTimestamp(CrdtFsConsolidationExample::union);

		// same with serializer for the timestamp container of the set of integers
		BinarySerializer_CrdtData<String, Set<Integer>> serializer =
				new BinarySerializer_CrdtData<>(UTF8_SERIALIZER, ofSet(INT_SERIALIZER));

		// create an FS-based CRDT client
		CrdtStorage_Fs<String, Set<Integer>> client =
				CrdtStorage_Fs.create(eventloop, fsClient, serializer, crdtFunction);
		//[END REGION_1]

		// wait for LocalActiveFs instance to start
		fsClient.start()
				.then(() -> {
					//[START REGION_2]
					// then upload two streams of items to it in parallel
					long timestamp = Reactor.getCurrentReactor().currentTimeMillis();
					Promise<Void> firstUpload =
							StreamSupplier.ofStream(Stream.of(
											new CrdtData<>("1_test_1", timestamp, Set.of(1, 2, 3)),
											new CrdtData<>("1_test_2", timestamp, Set.of(2, 3, 7)),
											new CrdtData<>("1_test_3", timestamp, Set.of(78, 2, 3)),
											new CrdtData<>("12_test_1", timestamp, Set.of(123, 124, 125)),
											new CrdtData<>("12_test_2", timestamp, Set.of(12))).sorted())
									.streamTo(StreamConsumer.ofPromise(client.upload()));

					timestamp += 100;
					Promise<Void> secondUpload =
							StreamSupplier.ofStream(Stream.of(
											new CrdtData<>("2_test_1", timestamp, Set.of(1, 2, 3)),
											new CrdtData<>("2_test_2", timestamp, Set.of(2, 3, 4)),
											new CrdtData<>("2_test_3", timestamp, Set.of(0, 1, 2)),
											new CrdtData<>("12_test_1", timestamp, Set.of(123, 542, 125, 2)),
											new CrdtData<>("12_test_2", timestamp, Set.of(12, 13))).sorted())
									.streamTo(StreamConsumer.ofPromise(client.upload()));
					//[END REGION_2]

					//[START REGION_3]
					// and wait for both of uploads to finish
					return Promises.all(firstUpload, secondUpload);
				})
				.whenComplete(() -> {

					// all the operations are async, but we run them sequentially
					// because we need to see the file list exactly before and after
					// consolidation process
					Promises.sequence(
							// here we can see that two files were created, one for each upload
							() -> fsClient.list("**")
									.whenResult(res -> System.out.println("\n" + res + "\n"))
									.toVoid(),

							// run the consolidation process
							client::consolidate,

							// now we can see that there is only one file left, and its size is
							// less than the sum of the sizes of the two files from above
							() -> fsClient.list("**")
									.whenResult(res -> System.out.println("\n" + res + "\n"))
									.toVoid()
					);
				});

		// all the above will not run until we actually start the eventloop
		eventloop.run();
		// shutdown the executor after the eventloop finishes (meaning there is no more work to do)
		// because executor waits for 60 seconds of being idle until it shuts down on its own
		executor.shutdown();
		//[END REGION_3]
	}
}
