package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.StreamSchema_Record;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;

final class StreamSchemaCodecModule extends AbstractModule {
	@Provides
	@Subtype(1)
	StreamCodec<StreamSchema_Record> recordStreamSchema(StreamCodec<RecordScheme> recordSchemeStreamCodec) {
		return StreamCodec.create(StreamSchema_Record::create,
				StreamSchema_Record::getRecordScheme, recordSchemeStreamCodec
		);
	}
}
