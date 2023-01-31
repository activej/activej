package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.join.RecordLeftJoiner;
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
	StreamCodec<RecordLeftJoiner> leftJoinerStreamCodec(StreamCodec<RecordScheme> recordSchemeStreamCodec) {
		return StreamCodec.create(RecordLeftJoiner::create,
				RecordLeftJoiner::getJoinRelType, StreamCodecs.ofEnum(JoinRelType.class),
				RecordLeftJoiner::getScheme, recordSchemeStreamCodec,
				RecordLeftJoiner::getLeft, recordSchemeStreamCodec,
				RecordLeftJoiner::getRight, recordSchemeStreamCodec
		);
	}
}
