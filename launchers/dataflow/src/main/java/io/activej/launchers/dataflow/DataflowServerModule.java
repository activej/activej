package io.activej.launchers.dataflow;

import io.activej.config.Config;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.dataflow.graph.Task;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.inject.SortingExecutor;
import io.activej.dataflow.messaging.DataflowRequest;
import io.activej.dataflow.messaging.DataflowResponse;
import io.activej.dataflow.node.NodeSort;
import io.activej.datastream.processor.AsyncStreamSorterStorage;
import io.activej.datastream.processor.StreamSorterStorage;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.getExecutor;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;

public final class DataflowServerModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(DataflowServerModule.class);

	private DataflowServerModule() {
	}

	public static DataflowServerModule create() {
		return new DataflowServerModule();
	}

	@Override
	protected void configure() {
		install(DataflowModule.create());
		install(DatasetIdModule.create());
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	@Eager
	@SortingExecutor
	Executor sortingExecutor(Config config) {
		return getExecutor(config.getChild("sortingExecutor"));
	}

	@Provides
	DataflowServer server(@Named("Dataflow") NioReactor reactor, Config config, ByteBufsCodec<DataflowRequest, DataflowResponse> codec, BinarySerializerLocator serializers, Injector environment) {
		return DataflowServer.create(reactor, codec, serializers, environment)
				.withInitializer(ofAbstractServer(config.getChild("dataflow.server")))
				.withInitializer(s -> s.withSocketSettings(s.getSocketSettings().withTcpNoDelay(true)));
	}

	@Provides
	@Eager
	DataflowClient client(@Named("Dataflow") NioReactor reactor, ByteBufsCodec<DataflowResponse, DataflowRequest> codec,
			BinarySerializerLocator serializers
	) {
		return DataflowClient.create(reactor, codec, serializers);
	}

	@Provides
	@Eager
	NodeSort.StreamSorterStorageFactory storageFactory(Executor executor, BinarySerializerLocator serializerLocator, Config config) throws IOException {
		Path providedSortDir = config.get(ofPath(), "dataflow.sortDir", null);
		Path sortDir = providedSortDir == null ? Files.createTempDirectory("dataflow-sort-dir") : providedSortDir;
		return new NodeSort.StreamSorterStorageFactory() {
			int index;

			@Override
			public <T> AsyncStreamSorterStorage<T> create(StreamSchema<T> streamSchema, Task context, Promise<Void> taskExecuted) {
				Path taskSortDir = sortDir.resolve(context.getTaskId() + "_" + index++);
				return StreamSorterStorage.create(executor, streamSchema.createSerializer(serializerLocator), LZ4FrameFormat.create(), taskSortDir);
			}

			@Override
			public <T> Promise<Void> cleanup(AsyncStreamSorterStorage<T> storage) {
				assert storage instanceof StreamSorterStorage<T>;
				StreamSorterStorage<T> storageImpl = (StreamSorterStorage<T>) storage;

				return Promise.ofBlocking(executor, () -> {
					try {
						Files.deleteIfExists(storageImpl.getPath());
					} catch (IOException e) {
						logger.warn("Could not delete sort storage directory: {}", storageImpl.getPath(), e);
					}
				});
			}
		};
	}
}
