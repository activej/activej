package module;

import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.dataflow.calcite.inject.CalciteServerModule;
import io.activej.dataflow.inject.DatasetId;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.StreamSupplierWithResult;
import io.activej.fs.AsyncFs;
import io.activej.fs.LocalFs;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.AbstractModule;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogNamingScheme;
import io.activej.multilog.AsyncMultilog;
import io.activej.multilog.ReactiveMultilog;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.serializer.BinarySerializer;
import misc.LogItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.common.Checks.checkState;
import static module.MultilogDataflowSchemaModule.LOG_ITEM_TABLE_NAME;

public class MultilogDataflowServerModule extends AbstractModule {

	private MultilogDataflowServerModule() {
	}

	public static MultilogDataflowServerModule create() {
		return new MultilogDataflowServerModule();
	}

	@Override
	protected void configure() {
		install(CalciteServerModule.create());
		install(MultilogDataflowSchemaModule.create());
	}

	@Provides
	@Transient
	@DatasetId(LOG_ITEM_TABLE_NAME)
	Promise<StreamSupplier<LogItem>> logItemDataset(@Named("Dataflow") Reactor reactor, AsyncMultilog<LogItem> logItemMultilog, @Named("partition") String partition) {
		checkState(reactor.inReactorThread());

		return logItemMultilog.read(partition, new LogFile("", 0), 0L, null)
				.map(StreamSupplierWithResult::getSupplier);
	}

	@Provides
	@Named("partition")
	String generateLogPartition() {
		int partitionId = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
		return "partition" + partitionId;
	}

	@Provides
	AsyncMultilog<LogItem> multilog(@Named("Dataflow") Reactor reactor, AsyncFs fs, FrameFormat frameFormat, BinarySerializer<LogItem> logItemSerializer, LogNamingScheme namingScheme) {
		return ReactiveMultilog.create(reactor, fs, frameFormat, logItemSerializer, namingScheme);
	}

	@Provides
	AsyncFs fs(@Named("Dataflow") Reactor reactor, Executor executor) throws IOException {
		Path multilogPath = Files.createTempDirectory("multilog");
		return LocalFs.create(reactor, executor, multilogPath);
	}

	@Provides
	FrameFormat frameFormat() {
		return LZ4FrameFormat.create();
	}

	@Provides
	LogNamingScheme logNamingScheme() {
		return LogNamingScheme.NAME_PARTITION_REMAINDER;
	}
}
