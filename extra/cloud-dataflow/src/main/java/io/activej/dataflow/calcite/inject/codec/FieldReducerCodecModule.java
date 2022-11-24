package io.activej.dataflow.calcite.inject.codec;

import io.activej.common.tuple.TupleConstructor2;
import io.activej.dataflow.calcite.aggregation.*;
import io.activej.dataflow.codec.StructuredStreamCodec;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

final class FieldReducerCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<MaxReducer<?>> maxReducer() {
		return fieldReducerCodec(MaxReducer::new);
	}

	@Provides
	@Subtype(1)
	StreamCodec<MinReducer<?>> minReducer() {
		return fieldReducerCodec(MinReducer::new);
	}

	@Provides
	@Subtype(2)
	StreamCodec<SumReducerDecimal<?>> sumReducerDecimal() {
		return fieldReducerCodec(SumReducerDecimal::new);
	}

	@Provides
	@Subtype(3)
	StreamCodec<SumReducerInteger<?>> sumReducerInteger() {
		return fieldReducerCodec(SumReducerInteger::new);
	}

	@Provides
	@Subtype(4)
	StreamCodec<AvgReducer> avgReducer() {
		return fieldReducerCodec(AvgReducer::new);
	}

	@Provides
	@Subtype(5)
	StreamCodec<CountReducer<?>> countReducer() {
		return fieldReducerCodec(CountReducer::new);
	}

	@Provides
	@Subtype(6)
	StreamCodec<KeyReducer<?>> keyReducer() {
		return fieldReducerCodec(KeyReducer::new);
	}

	private static <R extends FieldReducer<?, ?, ?>> StreamCodec<R> fieldReducerCodec(TupleConstructor2<Integer, String, R> constructor) {
		return StructuredStreamCodec.create(constructor,
				R::getFieldIndex, StreamCodecs.ofVarInt(),
				R::getFieldAlias, StreamCodecs.ofNullable(StreamCodecs.ofString())
		);
	}
}
