package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.RecordStreamSchema;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;

public final class StreamSchemaCodecModule extends AbstractModule {
	@Provides
	@Subtype(1)
	StreamCodec<RecordStreamSchema> recordStreamSchema(StreamCodec<RecordScheme> recordSchemeStreamCodec) {
		return StreamCodec.create(RecordStreamSchema::create,
			RecordStreamSchema::getRecordScheme, recordSchemeStreamCodec
		);
	}
}
