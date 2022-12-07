package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.graph.StreamSchemas;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StructuredStreamCodec;

import static io.activej.dataflow.codec.Utils.CLASS_STREAM_CODEC;

final class StreamSchemaCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<StreamSchemas.Simple<?>> simpleStreamSchema() {
		return StructuredStreamCodec.create(StreamSchemas::simple,
				StreamSchemas.Simple::getCls, CLASS_STREAM_CODEC
		);
	}
}
