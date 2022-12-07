package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.aggregation.FieldReducer;
import io.activej.dataflow.calcite.aggregation.RecordReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;
import io.activej.streamcodecs.StructuredStreamCodec;

import java.util.List;

final class RecordReducerCodecModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new FieldReducerCodecModule());
	}

	@Provides
	StreamCodec<StreamReducers.ReducerToResult<?, ?, ?, ?>> reducerToResult(
			StreamCodec<RecordScheme> recordSchemeStreamCodec,
			StreamCodec<FieldReducer<Object, Object, Object>> fieldReducerStreamCodec
	) {
		//noinspection unchecked,rawtypes
		return (StreamCodec) StructuredStreamCodec.create((value1, value2) -> RecordReducer.create(value1, ((List) value2)),
				RecordReducer::getOriginalScheme, recordSchemeStreamCodec,
				RecordReducer::getReducers, StreamCodecs.ofList(fieldReducerStreamCodec));
	}
}
