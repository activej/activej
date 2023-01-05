import io.activej.crdt.CrdtServer;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.crdt.function.CrdtFunction.ignoringTimestamp;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class CrdtExample {
	private static final CrdtDataSerializer<String, Integer> INTEGER_SERIALIZER = new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER);

	//[START REGION_2]
	private static final CrdtFunction<Integer> CRDT_FUNCTION = ignoringTimestamp(Integer::max);
	//[END REGION_2]

	private static final InetSocketAddress ADDRESS = new InetSocketAddress(5555);

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		//[START REGION_1]
		// create the 'remote' storage
		CrdtStorageMap<String, Integer> remoteStorage = CrdtStorageMap.create(eventloop, CRDT_FUNCTION);

		// put some default data into that storage
		remoteStorage.put("mx", 2);
		remoteStorage.put("test", 3);
		remoteStorage.put("test", 5);
		remoteStorage.put("only_remote", 35);
		remoteStorage.put("only_remote", 4);

		// and also output it for later comparison
		System.out.println("Data at 'remote' storage:");
		remoteStorage.iterator().forEachRemaining(System.out::println);
		System.out.println();

		// create and run a server for the 'remote' storage
		CrdtServer<String, Integer> server = CrdtServer.create(eventloop, remoteStorage, INTEGER_SERIALIZER)
				.withListenAddress(ADDRESS);
		server.listen();
		//[END REGION_1]

		//[START REGION_3]
		// now crate the client for that 'remote' storage
		AsyncCrdtStorage<String, Integer> client =
				CrdtStorageClient.create(eventloop, ADDRESS, INTEGER_SERIALIZER);

		// and also create the local storage
		CrdtStorageMap<String, Integer> localStorage =
				CrdtStorageMap.create(eventloop, CRDT_FUNCTION);

		// and fill it with some other values
		localStorage.put("mx", 22);
		// conflicting keys will be resolved with the crdt function
		localStorage.put("mx", 2);
		// so the actual value will be the max of all values of that key
		localStorage.put("mx", 23);
		localStorage.put("test", 1);
		localStorage.put("test", 2);
		localStorage.put("test", 4);
		localStorage.put("test", 3);
		localStorage.put("only_local", 47);
		localStorage.put("only_local", 12);

		// and output it too for later comparison
		System.out.println("Data at the local storage:");
		localStorage.iterator().forEachRemaining(System.out::println);
		System.out.println("\n");
		//[END REGION_3]

		//[START REGION_4]
		// now stream the local storage into the remote one through the TCP client-server pair
		StreamSupplier.ofPromise(localStorage.download())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(() -> {

					// check what is now at the 'remote' storage, the output should differ
					System.out.println("Synced data at 'remote' storage:");
					remoteStorage.iterator().forEachRemaining(System.out::println);
					System.out.println();

					// and now do the reverse process
					StreamSupplier.ofPromise(client.download())
							.streamTo(StreamConsumer.ofPromise(localStorage.upload()))
							.whenComplete(() -> {
								// now output the local storage, should be identical to the remote one
								System.out.println("Synced data at the local storage:");
								localStorage.iterator().forEachRemaining(System.out::println);
								System.out.println();

								// also stop the server to let the program finish
								server.close();
							});
				});

		eventloop.run();
		//[END REGION_4]
	}
}
