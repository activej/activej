import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.LWWSet;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.storage.cluster.ClusterCrdtStorage;
import io.activej.crdt.storage.cluster.IDiscoveryService;
import io.activej.crdt.storage.cluster.RendezvousPartitionGroup;
import io.activej.crdt.storage.cluster.RendezvousPartitionScheme;
import io.activej.crdt.storage.local.FileSystemCrdtStorage;
import io.activej.crdt.util.CrdtDataBinarySerializer;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileSystem;
import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class CrdtClusterExample {
	private static final CrdtDataBinarySerializer<String, LWWSet<String>> SERIALIZER =
		new CrdtDataBinarySerializer<>(UTF8_SERIALIZER, new LWWSet.Serializer<>(UTF8_SERIALIZER));

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.builder()
			.withCurrentThread()
			.build();
		ExecutorService executor = Executors.newSingleThreadExecutor();

		//[START REGION_1]
		// we create a list of 10 local partitions with string partition ids and string keys
		// normally all of them would be network clients for remote partitions
		Map<String, ICrdtStorage<String, LWWSet<String>>> clients = new HashMap<>();
		List<Promise<Void>> fsStartPromises = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			String id = "partition" + i;
			Path storage = Files.createTempDirectory("storage_" + id);
			FileSystem fs = FileSystem.create(eventloop, executor, storage);
			fsStartPromises.add(fs.start());
			clients.put(id, FileSystemCrdtStorage.create(eventloop, fs, SERIALIZER));
		}

		// grab a couple of them to work with
		ICrdtStorage<String, LWWSet<String>> partition3 = clients.get("partition3");
		ICrdtStorage<String, LWWSet<String>> partition6 = clients.get("partition6");

		// create a cluster with string keys, string partition ids,
		// and with replication count of 5 meaning that uploading items to the
		// cluster will make 5 copies of them across known partitions
		ClusterCrdtStorage<String, LWWSet<String>, String> cluster = ClusterCrdtStorage.<String, LWWSet<String>, String>create(
			eventloop,
			IDiscoveryService.of(RendezvousPartitionScheme.<String>builder()
				.withPartitionGroup(RendezvousPartitionGroup.builder(clients.keySet())
					.withReplicas(5)
					.build())
				.withCrdtProvider(clients::get)
				.build()),
			CrdtFunction.ofCrdtType());

		//[END REGION_1]
		// Here we will prepopulate two partitions with some sets of items
		//
		// * partition3:
		//   first = {#1, #2, #3, #4}
		//   second = {"#3", "#4", "#5", "#6"}
		//
		// * partition6:
		//   first = {#3, #4, #5, #6}
		//   second = {#2, #4, <removed #5>, <removed #6>}
		//
		// * expected result when downloading from the cluster:
		//   first = {#1, #2, #3, #4, #5, #6}
		//   second = {#2, #3, #4}

		//[START REGION_2]
		// sets on partition3
		CrdtData<String, LWWSet<String>> firstOn3 = new CrdtData<>("first", System.currentTimeMillis(), LWWSet.of("#1", "#2", "#3", "#4"));
		CrdtData<String, LWWSet<String>> secondOn3 = new CrdtData<>("second", System.currentTimeMillis(), LWWSet.of("#3", "#4", "#5", "#6"));

		// sets on partition6
		CrdtData<String, LWWSet<String>> firstOn6 = new CrdtData<>("first", System.currentTimeMillis(), LWWSet.of("#3", "#4", "#5", "#6"));

		// current implementation of LWWSet depends on system time
		// so to make the below removes with a higher timestamp, we wait for just a bit
		try {
			Thread.sleep(1);
		} catch (InterruptedException ignored) {
			Thread.currentThread().interrupt();
		}

		LWWSet<String> set = LWWSet.of("#2", "#4");
		set.remove("#5");
		set.remove("#6");
		CrdtData<String, LWWSet<String>> secondOn6 = new CrdtData<>("second", System.currentTimeMillis(), set);
		//[END REGION_2]

		//[START REGION_3]
		// wait for LocalActiveFs instances to start
		Promises.all(fsStartPromises)
			.then(() -> {
				// then upload these sets to both partition3 and partition6
				Promise<Void> uploadTo3 = StreamSuppliers.ofValues(firstOn3, secondOn3)
					.streamTo(StreamConsumers.ofPromise(partition3.upload()));
				Promise<Void> uploadTo6 = StreamSuppliers.ofValues(firstOn6, secondOn6)
					.streamTo(StreamConsumers.ofPromise(partition6.upload()));

				// wait for both of uploads to finish
				return Promises.all(uploadTo3, uploadTo6);
			})
			// and then download items from the cluster, and wait for result
			.then(cluster::start)
			.then(() -> cluster.download())
			// also collecting it to list
			.then(StreamSupplier::toList)
			// and then print the resulting list of items, it should match the expectation from above
			// (remember that sets are unordered, so you may not see it exactly as above)
			.whenComplete((list, $) -> System.out.println(list + "\n"));

		// actually run the eventloop and then shutdown the executor allowing the program to finish
		eventloop.run();
		executor.shutdown();
		//[END REGION_3]
	}
}
