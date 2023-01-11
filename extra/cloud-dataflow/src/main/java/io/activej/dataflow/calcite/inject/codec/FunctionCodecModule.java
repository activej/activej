package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.utils.IdentityFunction;
import io.activej.dataflow.calcite.utils.RecordFunction_Named;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

final class FunctionCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<RecordFunction_Named<?>> namedRecordFunction(DataflowSchema schema) {
		return new NamedRecordFunctionStreamCodec(schema);
	}

	@Provides
	@Subtype(1)
	StreamCodec<IdentityFunction<?>> identityFunction() {
		return StreamCodecs.singleton(IdentityFunction.getInstance());
	}

	@Provides
	@Subtype(2)
	StreamCodec<RecordProjectionFn> recordProjectionFn(StreamCodec<Operand<?>> operandStreamCodec) {
		return StreamCodec.create(RecordProjectionFn::create,
				RecordProjectionFn::getFieldProjections, StreamCodecs.ofList(
						StreamCodec.create(FieldProjection::new,
								FieldProjection::operand, operandStreamCodec,
								FieldProjection::fieldName, StreamCodecs.ofNullable(StreamCodecs.ofString())
						)
				)
		);
	}
}
