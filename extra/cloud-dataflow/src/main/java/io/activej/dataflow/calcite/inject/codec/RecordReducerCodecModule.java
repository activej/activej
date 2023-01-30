package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.aggregation.FieldReducer;
import io.activej.dataflow.calcite.aggregation.RecordReducer;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

import java.util.List;

public final class RecordReducerCodecModule extends AbstractModule {
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
		return (StreamCodec) StreamCodec.create((value1, value2) -> RecordReducer.create(value1, ((List) value2)),
				RecordReducer::getOriginalScheme, recordSchemeStreamCodec,
				RecordReducer::getReducers, StreamCodecs.ofList(fieldReducerStreamCodec));
	}
}
