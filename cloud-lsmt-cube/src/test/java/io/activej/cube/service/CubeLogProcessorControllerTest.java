package io.activej.cube.service;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.Cube;
import io.activej.cube.IdGeneratorStub;
import io.activej.cube.LogItem;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffCodec;
import io.activej.cube.ot.CubeOT;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.*;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileMetadata;
import io.activej.fs.LocalActiveFs;
import io.activej.multilog.Multilog;
import io.activej.multilog.MultilogImpl;
import io.activej.ot.OTCommit;
import io.activej.ot.OTStateManager;
import io.activej.ot.repository.OTRepositoryMySql;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplinkImpl;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.cube.TestUtils.initializeRepository;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.dataSource;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class CubeLogProcessorControllerTest {
	private static final OTSystem<LogDiff<CubeDiff>> OT_SYSTEM = LogOT.createLogOT(CubeOT.createCubeOT());

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private Multilog<LogItem> multilog;
	private LocalActiveFs logsFs;
	private CubeLogProcessorController<Long, Long> controller;

	@Before
	public void setUp() throws Exception {
		DataSource dataSource = dataSource("test.properties");
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		Eventloop eventloop = Eventloop.getCurrentEventloop();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		LocalActiveFs aggregationFs = LocalActiveFs.create(eventloop, executor, aggregationsDir);
		await(aggregationFs.start());
		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(),
				LZ4FrameFormat.create(), aggregationFs);
		Cube cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", ofLocalDate())
				.withDimension("advertiser", ofInt())
				.withDimension("campaign", ofInt())
				.withDimension("banner", ofInt())
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withAggregation(id("detailed")
						.withDimensions("date", "advertiser", "campaign", "banner")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("advertiser")
						.withDimensions("advertiser")
						.withMeasures("impressions", "clicks", "conversions", "revenue"));

		OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, executor, dataSource, new IdGeneratorStub(),
				OT_SYSTEM, LogDiffCodec.create(CubeDiffCodec.create(cube)));
		initializeRepository(repository);

		OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> uplink = OTUplinkImpl.create(repository, OT_SYSTEM);
		LogOTState<CubeDiff> logState = LogOTState.create(cube);
		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(eventloop, OT_SYSTEM, uplink, logState);

		logsFs = LocalActiveFs.create(eventloop, executor, logsDir);
		await(logsFs.start());
		BinarySerializer<LogItem> serializer = SerializerBuilder.create(classLoader)
				.build(LogItem.class);
		multilog = MultilogImpl.create(eventloop, logsFs, LZ4FrameFormat.create(), serializer, NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<LogItem, CubeDiff> logProcessor = LogOTProcessor.create(
				eventloop,
				multilog,
				cube.logStreamConsumer(LogItem.class),
				"test",
				singletonList("partitionA"),
				logState);

		controller = CubeLogProcessorController.create(
				eventloop,
				logState,
				stateManager,
				aggregationChunkStorage,
				singletonList(logProcessor));


		await(stateManager.checkout());
	}

	@Test
	public void testMalformedLogs() {
		await(StreamSupplier.of(new LogItem("test")).streamTo(
				StreamConsumer.ofPromise(multilog.write("partitionA"))));

		Map<String, FileMetadata> files = await(logsFs.list("**"));
		assertEquals(1, files.size());


		String logFile = first(files.keySet());
		ByteBuf serializedData = await(logsFs.download(logFile).then(supplier -> supplier
				.transformWith(ChannelFrameDecoder.create(LZ4FrameFormat.create())
						.withDecoderResets())
				.toCollector(ByteBufQueue.collector())));

		// offset right before string
		int bufSize = serializedData.readRemaining();
		serializedData.tail(serializedData.head() + 49);

		byte[] malformed = new byte[bufSize - 49];
		malformed[0] = 127; // exceeds message size
		await(ChannelSupplier.of(serializedData, ByteBuf.wrapForReading(malformed))
				.transformWith(ChannelFrameEncoder.create(LZ4FrameFormat.create())
						.withEncoderResets())
				.streamTo(logsFs.upload(logFile)));

		CubeException exception = awaitException(controller.process());
		Throwable firstCause = exception.getCause();
		assertThat(firstCause, instanceOf(UnknownFormatException.class));
		assertThat(firstCause.getCause(), instanceOf(StringIndexOutOfBoundsException.class));
	}
}
