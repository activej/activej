package io.activej.dataflow.calcite.inject.codec;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.operand.*;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;

public final class OperandCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<Operand_Scalar> operandScalar() {
		return StreamCodec.create(Operand_Scalar::new,
				Operand_Scalar::getValue, StreamCodecs.ofNullable(Utils.VALUE_STREAM_CODEC)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<Operand_FieldAccess> fieldAccess(
			StreamCodec<Operand<?>> operandStreamCodec,
			DefiningClassLoader classLoader
	) {
		return StreamCodec.create((a, b) -> new Operand_FieldAccess(a, b, classLoader),
				Operand_FieldAccess::getObjectOperand, operandStreamCodec,
				Operand_FieldAccess::getFieldName, StreamCodecs.ofString()
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<Operand_RecordField> operandRecordField() {
		return StreamCodec.create(Operand_RecordField::new,
				Operand_RecordField::getIndex, StreamCodecs.ofVarInt()
		);
	}

	@Provides
	@Subtype(3)
	StreamCodec<Operand_Cast> operandCast(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Operand_Cast::new,
				Operand_Cast::getValueOperand, operandStreamCodec,
				Operand_Cast::getType, StreamCodecs.ofVarInt()
		);
	}

	@Provides
	@Subtype(4)
	StreamCodec<Operand_IfNull> operandIfNull(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Operand_IfNull::new,
				Operand_IfNull::getCheckedOperand, operandStreamCodec,
				Operand_IfNull::getDefaultValueOperand, operandStreamCodec
		);
	}

	@Provides
	@Subtype(5)
	StreamCodec<Operand_ListGet> operandListGet(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Operand_ListGet::new,
				Operand_ListGet::getListOperand, operandStreamCodec,
				Operand_ListGet::getIndexOperand, operandStreamCodec
		);
	}

	@Provides
	@Subtype(6)
	StreamCodec<Operand_MapGet> operandMapGet(
			StreamCodec<Operand<?>> operandStreamCodec
	) {
		return StreamCodec.create(Operand_MapGet::new,
				Operand_MapGet::getMapOperand, operandStreamCodec,
				Operand_MapGet::getKeyOperand, operandStreamCodec
		);
	}
}
