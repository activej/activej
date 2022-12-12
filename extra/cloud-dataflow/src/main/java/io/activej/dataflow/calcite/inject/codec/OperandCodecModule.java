package io.activej.dataflow.calcite.inject.codec;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.operand.*;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

final class OperandCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<OperandScalar> operandScalar() {
		return StreamCodec.create(OperandScalar::new,
				OperandScalar::getValue, StreamCodecs.ofNullable(Utils.VALUE_STREAM_CODEC)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<OperandFieldAccess> fieldAccess(
			StreamCodec<Operand<?>> operandStreamCodec,
			DefiningClassLoader classLoader
	) {
		return StreamCodec.create((a, b) -> new OperandFieldAccess(a, b, classLoader),
				OperandFieldAccess::getObjectOperand, operandStreamCodec,
				OperandFieldAccess::getFieldName, StreamCodecs.ofString()
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<OperandRecordField> operandRecordField() {
		return StreamCodec.create(OperandRecordField::new,
				OperandRecordField::getIndex, StreamCodecs.ofVarInt()
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<OperandCast> operandCast(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(OperandCast::new,
				OperandCast::getValueOperand, operandStreamCodec,
				OperandCast::getType, StreamCodecs.ofVarInt()
		);
	}

	@Provides
	@Subtype(4)
	StreamCodec<OperandIfNull> operandIfNull(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(OperandIfNull::new,
				OperandIfNull::getCheckedOperand, operandStreamCodec,
				OperandIfNull::getDefaultValueOperand, operandStreamCodec
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<OperandListGet> operandListGet(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(OperandListGet::new,
				OperandListGet::getListOperand, operandStreamCodec,
				OperandListGet::getIndexOperand, operandStreamCodec
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<OperandMapGet> operandMapGet(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(OperandMapGet::new,
				OperandMapGet::getMapOperand, operandStreamCodec,
				OperandMapGet::getKeyOperand, operandStreamCodec
		);
	}
}
