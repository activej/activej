package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.join.LeftJoiner_Record;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import org.apache.calcite.rel.core.JoinRelType;

public final class LeftJoinerCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<LeftJoiner_Record> leftJoinerStreamCodec(StreamCodec<RecordScheme> recordSchemeStreamCodec) {
		return StreamCodec.create(LeftJoiner_Record::create,
				LeftJoiner_Record::getJoinRelType, StreamCodecs.ofEnum(JoinRelType.class),
				LeftJoiner_Record::getScheme, recordSchemeStreamCodec,
				LeftJoiner_Record::getLeft, recordSchemeStreamCodec,
				LeftJoiner_Record::getRight, recordSchemeStreamCodec
		);
	}
}
