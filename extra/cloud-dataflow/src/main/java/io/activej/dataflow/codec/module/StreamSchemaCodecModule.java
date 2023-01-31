package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.graph.StreamSchemas;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;

import static io.activej.dataflow.codec.Utils.CLASS_STREAM_CODEC;

public final class StreamSchemaCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<StreamSchemas.SimpleStreamSchema<?>> simpleStreamSchema() {
		return StreamCodec.create(StreamSchemas::simple,
				StreamSchemas.SimpleStreamSchema::getCls, CLASS_STREAM_CODEC
		);
	}
}
