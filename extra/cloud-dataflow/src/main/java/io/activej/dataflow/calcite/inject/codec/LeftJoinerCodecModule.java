package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.join.RecordJoiner;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.RecordScheme;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;
import io.activej.streamcodecs.StructuredStreamCodec;
import org.apache.calcite.rel.core.JoinRelType;

final class LeftJoinerCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<RecordJoiner> leftJoinerStreamCodec(StreamCodec<RecordScheme> recordSchemeStreamCodec) {
		return StructuredStreamCodec.create(RecordJoiner::create,
				RecordJoiner::getJoinRelType, StreamCodecs.ofEnum(JoinRelType.class),
				RecordJoiner::getScheme, recordSchemeStreamCodec,
				RecordJoiner::getLeft, recordSchemeStreamCodec,
				RecordJoiner::getRight, recordSchemeStreamCodec
		);
	}
}
