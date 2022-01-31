package io.activej.cube.service;

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.frames.ChannelFrameDecoder;
import io.activej.csp.process.frames.ChannelFrameEncoder;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.Cube;
import io.activej.cube.CubeTestBase;
import io.activej.cube.IdGeneratorStub;
import io.activej.cube.LogItem;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.LogOTState;
import io.activej.fs.FileMetadata;
import io.activej.fs.LocalActiveFs;
import io.activej.multilog.Multilog;
import io.activej.multilog.MultilogImpl;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.OTUplink;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.common.Utils.first;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

public final class CubeLogProcessorControllerTest extends CubeTestBase {
	private Multilog<LogItem> multilog;
	private LocalActiveFs logsFs;
	private CubeLogProcessorController<Long, Long> controller;

	@Before
	public void setUp() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		LocalActiveFs aggregationFs = LocalActiveFs.create(EVENTLOOP, EXECUTOR, aggregationsDir);
		await(aggregationFs.start());
		AggregationChunkStorage<Long> aggregationChunkStorage = ActiveFsChunkStorage.create(EVENTLOOP, ChunkIdCodec.ofLong(), new IdGeneratorStub(),
				LZ4FrameFormat.create(), aggregationFs);
		Cube cube = Cube.create(EVENTLOOP, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
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

		OTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cube);

		LogOTState<CubeDiff> logState = LogOTState.create(cube);
		OTStateManager<Long, LogDiff<CubeDiff>> stateManager = OTStateManager.create(EVENTLOOP, LOG_OT, uplink, logState);

		logsFs = LocalActiveFs.create(EVENTLOOP, EXECUTOR, logsDir);
		await(logsFs.start());
		BinarySerializer<LogItem> serializer = SerializerBuilder.create(CLASS_LOADER)
				.build(LogItem.class);
		multilog = MultilogImpl.create(EVENTLOOP, logsFs, LZ4FrameFormat.create(), serializer, NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<LogItem, CubeDiff> logProcessor = LogOTProcessor.create(
				EVENTLOOP,
				multilog,
				cube.logStreamConsumer(LogItem.class),
				"test",
				List.of("partitionA"),
				logState);

		controller = CubeLogProcessorController.create(
				EVENTLOOP,
				logState,
				stateManager,
				aggregationChunkStorage,
				List.of(logProcessor));


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
				.toCollector(ByteBufs.collector())));

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
