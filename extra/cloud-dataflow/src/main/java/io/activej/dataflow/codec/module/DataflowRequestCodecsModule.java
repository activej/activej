package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.messaging.DataflowRequest.Download;
import io.activej.dataflow.messaging.DataflowRequest.Execute;
import io.activej.dataflow.messaging.DataflowRequest.GetTasks;
import io.activej.dataflow.messaging.DataflowRequest.Handshake;
import io.activej.dataflow.node.Node;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

import static io.activej.dataflow.codec.Utils.STREAM_ID_STREAM_CODEC;
import static io.activej.dataflow.codec.Utils.VERSION_STREAM_CODEC;

public final class DataflowRequestCodecsModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new NodeCodecModule());
	}

	@Provides
	@Subtype(0)
	StreamCodec<Handshake> handshake() {
		return StreamCodec.create(Handshake::new,
				Handshake::version, VERSION_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<Download> download() {
		return StreamCodec.create(Download::new,
				Download::streamId, STREAM_ID_STREAM_CODEC
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<Execute> execute(
			StreamCodec<Node> nodeStreamCodec
	) {
		return StreamCodec.create(Execute::new,
				Execute::taskId, StreamCodecs.ofVarLong(),
				Execute::nodes, StreamCodecs.ofList(nodeStreamCodec)
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<GetTasks> getTasks() {
		return StreamCodec.create(GetTasks::new,
				GetTasks::taskId, StreamCodecs.ofNullable(StreamCodecs.ofVarLong())
		);
	}
}
