package io.activej.dataflow.calcite.inject.codec;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.impl.*;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

public final class OperandCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<Scalar> operandScalar() {
		return StreamCodec.create(Scalar::new,
			scalar -> scalar.value, StreamCodecs.ofNullable(Utils.VALUE_STREAM_CODEC)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<FieldAccess> fieldAccess(StreamCodec<Operand<?>> operandStreamCodec, DefiningClassLoader classLoader) {
		return StreamCodec.create((a, b) -> new FieldAccess(a, b, classLoader),
			fieldAccess -> fieldAccess.objectOperand, operandStreamCodec,
			fieldAccess -> fieldAccess.fieldName, StreamCodecs.ofString()
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<RecordField> operandRecordField() {
		return StreamCodec.create(RecordField::new,
			recordField -> recordField.index, StreamCodecs.ofVarInt()
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<Cast> operandCast(StreamCodec<Operand<?>> operandStreamCodec) {
		return StreamCodec.create(Cast::new,
			cast -> cast.valueOperand, operandStreamCodec,
			cast -> cast.type, StreamCodecs.ofVarInt()
		);
	}

	@Provides
	@Subtype(4)
	StreamCodec<IfNull> operandIfNull(StreamCodec<Operand<?>> operandStreamCodec) {
		return StreamCodec.create(IfNull::new,
			ifNull -> ifNull.checkedOperand, operandStreamCodec,
			ifNull -> ifNull.defaultValueOperand, operandStreamCodec
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<ListGet> operandListGet(StreamCodec<Operand<?>> operandStreamCodec) {
		return StreamCodec.create(ListGet::new,
			listGet -> listGet.listOperand, operandStreamCodec,
			listGet -> listGet.indexOperand, operandStreamCodec
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<MapGet> operandMapGet(StreamCodec<Operand<?>> operandStreamCodec) {
		return StreamCodec.create(MapGet::new,
			mapGet -> mapGet.mapOperand, operandStreamCodec,
			mapGet -> mapGet.keyOperand, operandStreamCodec
		);
	}
}
