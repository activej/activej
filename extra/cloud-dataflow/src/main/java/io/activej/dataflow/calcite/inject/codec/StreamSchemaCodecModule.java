package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.RecordStreamSchema;
import io.activej.dataflow.codec.StructuredStreamCodec;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;

final class StreamSchemaCodecModule extends AbstractModule {
	@Provides
	@Subtype(1)
	StreamCodec<RecordStreamSchema> recordStreamSchema(StreamCodec<RecordScheme> recordSchemeStreamCodec) {
		return StructuredStreamCodec.create(RecordStreamSchema::create,
				RecordStreamSchema::getRecordScheme, recordSchemeStreamCodec
		);
	}
}
