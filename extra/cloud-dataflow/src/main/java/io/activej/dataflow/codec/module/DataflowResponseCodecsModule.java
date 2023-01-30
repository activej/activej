package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.graph.TaskStatus;
import io.activej.dataflow.messaging.DataflowResponse.*;
import io.activej.dataflow.stats.NodeStat;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

import static io.activej.dataflow.codec.Utils.INSTANT_STREAM_CODEC;
import static io.activej.dataflow.codec.Utils.VERSION_STREAM_CODEC;

public final class DataflowResponseCodecsModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new NodeStatCodecModule());
	}

	@Provides
	@Subtype(0)
	StreamCodec<Handshake> handshake() {
		return StreamCodec.create(Handshake::new,
				Handshake::handshakeFailure, StreamCodecs.ofNullable(
						StreamCodec.create(HandshakeFailure::new,
								HandshakeFailure::minimalVersion, VERSION_STREAM_CODEC,
								HandshakeFailure::message, StreamCodecs.ofString())
				)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<PartitionData> partitionData() {
		return StreamCodec.create(PartitionData::new,
				PartitionData::running, StreamCodecs.ofVarInt(),
				PartitionData::succeeded, StreamCodecs.ofVarInt(),
				PartitionData::failed, StreamCodecs.ofVarInt(),
				PartitionData::cancelled, StreamCodecs.ofVarInt(),
				PartitionData::lastTasks, StreamCodecs.ofList(
						StreamCodec.create(TaskDescription::new,
								TaskDescription::id, StreamCodecs.ofVarLong(),
								TaskDescription::status, StreamCodecs.ofEnum(TaskStatus.class)
						)
				)
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<Result> result() {
		return StreamCodec.create(Result::new,
				Result::error, StreamCodecs.ofNullable(StreamCodecs.ofString())
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<TaskData> taskData(
			StreamCodec<NodeStat> nodeStatStreamCodec
	) {
		return StreamCodec.create(TaskData::new,
				TaskData::status, StreamCodecs.ofEnum(TaskStatus.class),
				TaskData::startTime, StreamCodecs.ofNullable(INSTANT_STREAM_CODEC),
				TaskData::finishTime, StreamCodecs.ofNullable(INSTANT_STREAM_CODEC),
				TaskData::error, StreamCodecs.ofNullable(StreamCodecs.ofString()),
				TaskData::nodes, StreamCodecs.ofMap(StreamCodecs.ofVarInt(), nodeStatStreamCodec),
				TaskData::graphViz, StreamCodecs.ofString()
		);
	}
}
