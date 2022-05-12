package io.activej.dataflow.stream;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.function.FunctionEx;
import io.activej.common.ref.RefBoolean;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.dsl.ChannelSupplierTransformer;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.helper.PartitionedCollector;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.inject.DatasetId;
import io.activej.dataflow.node.PartitionedStreamConsumerFactory;
import io.activej.dataflow.node.PartitionedStreamSupplierFactory;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.FunctionSerializer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.processor.StreamUnion;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.fs.http.ActiveFsServlet;
import io.activej.fs.http.HttpActiveFs;
import io.activej.http.AsyncHttpClient;
import io.activej.http.AsyncHttpServer;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.net.AbstractServer;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.serializer.BinarySerializer;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.Utils.first;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.dataflow.dataset.Datasets.*;
import static io.activej.dataflow.proto.ProtobufUtils.ofObject;
import static io.activej.datastream.StreamSupplier.ofChannelSupplier;
import static io.activej.datastream.processor.StreamReducers.mergeReducer;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("rawtypes")
public final class PartitionedStreamTest {
	private static final String SOURCE_FILENAME = "data.txt";
	private static final String TARGET_FILENAME = "result.txt";
	private static final Random RANDOM = ThreadLocalRandom.current();
	private static final Function<String, Integer> KEY_FUNCTION = string -> Integer.valueOf(string.split(":")[1]);

	@ClassRule
	public static EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tempDir = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private Eventloop serverEventloop;
	private List<AsyncHttpServer> sourceFsServers;
	private List<AsyncHttpServer> targetFsServers;
	private List<DataflowServer> dataflowServers;

	@Before
	public void setUp() {
		sourceFsServers = new ArrayList<>();
		targetFsServers = new ArrayList<>();
		dataflowServers = new ArrayList<>();
		serverEventloop = Eventloop.create().withEventloopFatalErrorHandler(rethrow());
		serverEventloop.keepAlive(true);
		new Thread(serverEventloop).start();
	}

	@After
	public void tearDown() throws Exception {
		serverEventloop.submit(() -> {
			sourceFsServers.forEach(AbstractServer::close);
			targetFsServers.forEach(AbstractServer::close);
			dataflowServers.forEach(AbstractServer::close);
		}).get();
		serverEventloop.keepAlive(false);
		Thread serverEventloopThread = serverEventloop.getEventloopThread();
		if (serverEventloopThread != null) {
			serverEventloopThread.join();
		}
	}

	@Test
	public void testNotSortedEqual() throws IOException {
		launchServers(5, 5, 0, false);
		Map<Partition, List<String>> result = collectToMap(false);

		int serverIdx = 0;
		assertEquals(5, result.size());
		for (List<String> items : result.values()) {
			assertEquals(100, items.size());
			for (String item : items) {
				assertTrue(item.startsWith("Server" + serverIdx));
			}
			serverIdx++;
		}
	}

	@Test
	public void testNotSortedMoreFsServers() throws IOException {
		launchServers(10, 3, 0, false);
		Map<Partition, List<String>> result = collectToMap(false);

		assertEquals(3, result.size());

		List<String> firstPartition = get(result, 0);
		assertEquals(400, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0", "Server3", "Server6", "Server9");

		List<String> secondPartition = get(result, 1);
		assertEquals(300, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1", "Server4", "Server7");

		List<String> thirdPartition = get(result, 2);
		assertEquals(300, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2", "Server5", "Server8");
	}

	@Test
	public void testNotSortedMoreDataflowServers() throws IOException {
		launchServers(3, 10, 0, false);
		Map<Partition, List<String>> result = collectToMap(false);

		assertEquals(10, result.size());

		List<String> firstPartition = get(result, 0);
		assertEquals(100, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0");

		List<String> secondPartition = get(result, 1);
		assertEquals(100, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1");

		List<String> thirdPartition = get(result, 2);
		assertEquals(100, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2");

		for (int i = 3; i < 10; i++) {
			List<String> ithPartition = get(result, i);
			assertEquals(0, ithPartition.size());
		}
	}

	@Test
	public void testSortedEqual() throws IOException {
		launchServers(5, 5, 0, true);
		Map<Partition, List<String>> result = collectToMap(true);

		assertEquals(5, result.size());
		assertSorted(result.values());

		for (int i = 0; i < 5; i++) {
			List<String> ithPartition = get(result, i);
			assertEquals(100, ithPartition.size());
		}
	}

	@Test
	public void testSortedMoreFsServers() throws IOException {
		launchServers(10, 3, 0, true);
		Map<Partition, List<String>> result = collectToMap(true);

		assertEquals(3, result.size());
		assertSorted(result.values());

		List<String> firstPartition = get(result, 0);
		assertEquals(400, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0", "Server3", "Server6", "Server9");

		List<String> secondPartition = get(result, 1);
		assertEquals(300, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1", "Server4", "Server7");

		List<String> thirdPartition = get(result, 2);
		assertEquals(300, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2", "Server5", "Server8");
	}

	@Test
	public void testSortedMoreDataflowServers() throws IOException {
		launchServers(3, 10, 0, true);
		Map<Partition, List<String>> result = collectToMap(true);

		assertEquals(10, result.size());
		assertSorted(result.values());

		List<String> firstPartition = get(result, 0);
		assertEquals(100, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0");

		List<String> secondPartition = get(result, 1);
		assertEquals(100, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1");

		List<String> thirdPartition = get(result, 2);
		assertEquals(100, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2");

		for (int i = 3; i < 10; i++) {
			List<String> ithPartition = get(result, i);
			assertEquals(0, ithPartition.size());
		}
	}

	@Test
	public void testPropagationToTargetFs() throws IOException {
		launchServers(10, 2, 5, false);

		filterOddAndPropagateToTarget();

		Set<String> allTargetItems = new HashSet<>();
		for (int i = 0; i < targetFsServers.size(); i++) {
			ActiveFs fs = createClient(Eventloop.getCurrentEventloop(), targetFsServers.get(i));
			List<String> items = new ArrayList<>();
			await(fs.download(TARGET_FILENAME)
					.then(supplier -> supplier.transformWith(new CSVDecoder())
							.streamTo(StreamConsumerToList.create(items))));
			for (String item : items) {
				allTargetItems.add(item);
				Integer value = KEY_FUNCTION.apply(item);
				assertEquals(0, value % 2);
				int serverId = Integer.parseInt(item.substring(6, 7));
				assertEquals(serverId % 2, i % 2);
			}
		}

		Set<String> sourceFiltered = await(Promises.toList(sourceFsServers.stream()
						.map(server -> createClient(Eventloop.getCurrentEventloop(), server))
						.map(client -> client.download(SOURCE_FILENAME)
								.then(supplier -> supplier
										.transformWith(new CSVDecoder())
										.toList())))
				.map(lists -> lists.stream()
						.flatMap(Collection::stream)
						.filter(new IsEven())
						.collect(toSet())));

		assertEquals(sourceFiltered, allTargetItems);
	}

	// region modules
	private Module createServerModule() {
		return Modules.combine(
				DataflowModule.create(),
				createSerializersModule(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return serverEventloop;
					}

					@Provides
					DataflowServer server(Eventloop eventloop, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator locator, Injector injector, FunctionSerializer functionSerializer) {
						return DataflowServer.create(eventloop, codec, locator, injector, functionSerializer);
					}

					@Provides
					@Named("source")
					List<ActiveFs> sourceFss(Eventloop eventloop) {
						return createClients(eventloop, sourceFsServers);
					}

					@Provides
					@Named("target")
					List<ActiveFs> targetFss(Eventloop eventloop) {
						return createClients(eventloop, targetFsServers);
					}

					@Provides
					@DatasetId("data source")
					PartitionedStreamSupplierFactory<String> data(@Named("source") List<ActiveFs> activeFss) {
						return (partitionIndex, maxPartitions) -> {
							StreamUnion<String> union = StreamUnion.create();
							for (int i = partitionIndex; i < activeFss.size(); i += maxPartitions) {
								ChannelSupplier.ofPromise(activeFss.get(i).download(SOURCE_FILENAME))
										.transformWith(new CSVDecoder())
										.streamTo(union.newInput());
							}
							return union.getOutput();
						};
					}

					@Provides
					@DatasetId("sorted data source")
					PartitionedStreamSupplierFactory<String> dataSorted(@Named("source") List<ActiveFs> activeFss) {
						return (partitionIndex, maxPartitions) -> {
							StreamReducer<Integer, String, Void> merger = StreamReducer.create();

							for (int i = partitionIndex; i < activeFss.size(); i += maxPartitions) {
								ChannelSupplier.ofPromise(activeFss.get(i).download(SOURCE_FILENAME))
										.transformWith(new CSVDecoder())
										.streamTo(merger.newInput(KEY_FUNCTION, mergeReducer()));
							}
							return merger.getOutput();
						};
					}

					@Provides
					@DatasetId("data target")
					PartitionedStreamConsumerFactory<String> dataUpload(@Named("target") List<ActiveFs> activeFss) {
						return (partitionIndex, maxPartitions) -> {
							StreamSplitter<String, String> splitter = StreamSplitter.create((item, acceptors) ->
									acceptors[item.hashCode() % acceptors.length].accept(item));

							List<Promise<Void>> uploads = new ArrayList<>();
							for (int i = partitionIndex; i < activeFss.size(); i += maxPartitions) {
								uploads.add(splitter.newOutput()
										.streamTo(ChannelConsumer.ofPromise(activeFss.get(i).upload(TARGET_FILENAME))
												.transformWith(new CSVEncoder())));
							}

							return splitter.getInput()
									.withAcknowledgement(ack -> ack.both(Promises.all(uploads)));
						};
					}

				}
		);
	}

	private static Module createClientModule() {
		return Modules.combine(
				DataflowModule.create(),
				createSerializersModule(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.getCurrentEventloop();
					}

					@Provides
					DataflowClient client(Executor executor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec, BinarySerializerLocator locator, FunctionSerializer functionSerializer) throws IOException {
						return new DataflowClient(executor, Files.createTempDirectory("").toAbsolutePath(), codec, locator, functionSerializer);
					}

					@Provides
					Executor executor() {
						return newSingleThreadExecutor();
					}
				}
		);
	}

	private static Module createSerializersModule() {
		return new AbstractModule() {
			@Provides
			BinarySerializer<Predicate<?>> isEvenCodec() {
				return ofObject(IsEven::new);
			}
		};
	}
	// endregion

	// region helpers
	private static List<ActiveFs> createClients(Eventloop eventloop, List<AsyncHttpServer> servers) {
		return servers.stream()
				.map(server -> createClient(eventloop, server))
				.collect(Collectors.toList());
	}

	private static ActiveFs createClient(Eventloop eventloop, AsyncHttpServer server) {
		int port = server.getListenAddresses().get(0).getPort();
		return HttpActiveFs.create("http://localhost:" + port, AsyncHttpClient.create(eventloop));
	}

	private void assertSorted(Collection<List<String>> result) {
		for (List<String> items : result) {
			int lastKey = 0;
			for (String item : items) {
				int key = KEY_FUNCTION.apply(item);
				assertTrue(key >= lastKey);
				lastKey = key;
			}
		}
	}

	private static List<String> get(Map<Partition, List<String>> result, int idx) {
		Iterator<List<String>> iterator = result.values().iterator();
		for (int i = 0; i < idx + 1; i++) {
			List<String> list = iterator.next();
			if (i == idx) {
				return list;
			}
		}
		throw new AssertionError();
	}

	private static void assertItemPrefixes(List<String> items, String... prefixes) {
		Map<String, List<String>> collected = items.stream().collect(groupingBy(item -> item.split(":")[0]));
		assertEquals(Set.of(prefixes), collected.keySet());
		int size = first(collected.values()).size();
		assertTrue(size > 0);
		for (List<String> value : collected.values()) {
			assertEquals(size, value.size());
		}
	}

	private Map<Partition, List<String>> collectToMap(boolean sorted) {
		Injector injector = Injector.of(createClientModule());
		DataflowClient client = injector.getInstance(DataflowClient.class);
		DataflowGraph graph = new DataflowGraph(client, toPartitions(dataflowServers));
		Dataset<String> compoundDataset = datasetOfId(sorted ? "sorted data source" : "data source", String.class);

		PartitionedCollector<String> collector = new PartitionedCollector<>(compoundDataset, client);

		Promise<Map<Partition, List<String>>> resultPromise = collector.compile(graph);
		await(graph.execute());
		return await(resultPromise);
	}

	private void filterOddAndPropagateToTarget() {
		Injector injector = Injector.of(createClientModule());
		DataflowClient client = injector.getInstance(DataflowClient.class);
		DataflowGraph graph = new DataflowGraph(client, toPartitions(dataflowServers));
		Dataset<String> compoundDataset = datasetOfId("data source", String.class);
		Dataset<String> filteredDataset = filter(compoundDataset, new IsEven());
		Dataset<String> consumerDataset = consumerOfId(filteredDataset, "data target");
		consumerDataset.channels(DataflowContext.of(graph));

		await(graph.execute());
	}

	private void launchServers(int nSourceFsServers, int nDataflowServers, int nTargetFsServers, boolean sorted) throws IOException {
		sourceFsServers.addAll(launchSourceFsServers(nSourceFsServers, sorted));
		targetFsServers.addAll(launchTargetFsServers(nTargetFsServers));
		dataflowServers.addAll(launchDataflowServers(nDataflowServers));
	}

	private List<AsyncHttpServer> launchSourceFsServers(int nServers, boolean sorted) throws IOException {
		List<AsyncHttpServer> servers = new ArrayList<>();
		for (int i = 0; i < nServers; i++) {
			Path tmp = tempDir.newFolder("source_server_" + i + "_").toPath();
			writeDataFile(tmp, i, sorted);
			LocalActiveFs fsClient = LocalActiveFs.create(serverEventloop, newSingleThreadExecutor(), tmp);
			await(fsClient.start());
			AsyncHttpServer server = AsyncHttpServer.create(serverEventloop, ActiveFsServlet.create(fsClient));
			servers.add(server);
		}
		for (AsyncHttpServer server : servers) {
			listen(server.withListenPort(getFreePort()));
		}
		return servers;
	}

	private List<AsyncHttpServer> launchTargetFsServers(int nServers) throws IOException {
		List<AsyncHttpServer> servers = new ArrayList<>();
		for (int i = 0; i < nServers; i++) {
			Path tmp = tempDir.newFolder("target_server_" + i + "_").toPath();
			LocalActiveFs fsClient = LocalActiveFs.create(serverEventloop, newSingleThreadExecutor(), tmp);
			await(fsClient.start());
			AsyncHttpServer server = AsyncHttpServer.create(serverEventloop, ActiveFsServlet.create(fsClient));
			servers.add(server);
		}
		for (AsyncHttpServer server : servers) {
			listen(server.withListenPort(getFreePort()));
		}
		return servers;
	}

	private static void writeDataFile(Path serverFsPath, int serverIdx, boolean sorted) throws IOException {
		int nItems = 100;
		int nextNumber = RANDOM.nextInt(10);

		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < nItems; i++) {
			if (sorted) {
				nextNumber += RANDOM.nextInt(10);
			} else {
				nextNumber = RANDOM.nextInt(1000);
			}
			stringBuilder.append("Server" + serverIdx + ":" + nextNumber);
			if (i != nItems - 1) {
				stringBuilder.append(',');
			}
		}
		Path path = serverFsPath.resolve(SOURCE_FILENAME);
		Files.writeString(path, stringBuilder.toString());
	}

	private List<DataflowServer> launchDataflowServers(int nPartitions) {
		Module serverModule = createServerModule();
		List<DataflowServer> servers = new ArrayList<>();
		for (int i = 0; i < nPartitions; i++) {
			Injector injector = Injector.of(serverModule);
			DataflowServer server = injector.getInstance(DataflowServer.class);
			server.withListenPort(getFreePort());
			listen(server);
			servers.add(server);
		}
		return servers;
	}

	private void listen(AbstractServer<?> server) {
		try {
			serverEventloop.submit(() -> {
				try {
					server.listen();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		} catch (ExecutionException e) {
			throw new AssertionError(e);
		}
	}

	private static List<Partition> toPartitions(List<DataflowServer> servers) {
		return servers.stream()
				.map(AbstractServer::getListenAddresses)
				.flatMap(Collection::stream)
				.map(Partition::new)
				.collect(toList());
	}
	// endregion

	private static class CSVDecoder implements ChannelSupplierTransformer<ByteBuf, StreamSupplier<String>> {

		@Override
		public StreamSupplier<String> transform(ChannelSupplier<ByteBuf> supplier) {
			BinaryChannelSupplier binaryChannelSupplier = BinaryChannelSupplier.of(supplier);
			return ofChannelSupplier(ChannelSupplier.of(
					() -> binaryChannelSupplier.decode(
									bufs -> {
										for (int i = 0; i < bufs.remainingBytes(); i++) {
											if (bufs.peekByte(i) == ',') {
												ByteBuf buf = bufs.takeExactSize(i);
												bufs.skip(1);
												return buf.asString(UTF_8);
											}
										}
										return null;
									})
							.map(FunctionEx.identity(),
									e -> {
										if (e instanceof TruncatedDataException) {
											ByteBufs bufs = binaryChannelSupplier.getBufs();
											return bufs.isEmpty() ? null : bufs.takeRemaining().asString(UTF_8);
										}
										throw e;
									}),
					binaryChannelSupplier));
		}
	}

	private static final class CSVEncoder implements ChannelConsumerTransformer<ByteBuf, StreamConsumer<String>> {
		@Override
		public StreamConsumer<String> transform(ChannelConsumer<ByteBuf> consumer) {
			RefBoolean first = new RefBoolean(true);
			return StreamConsumer.ofChannelConsumer(consumer
					.map(item -> {
						if (first.get()) {
							first.flip();
							return wrapUtf8(item);
						}
						return wrapUtf8("," + item);
					}));
		}
	}

	private static final class IsEven implements Predicate<String> {

		@Override
		public boolean test(String string) {
			return KEY_FUNCTION.apply(string) % 2 == 0;
		}
	}
}
