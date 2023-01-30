package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.utils.RecordKeyComparator;
import io.activej.dataflow.calcite.utils.RecordSortComparator;
import io.activej.dataflow.calcite.utils.RecordSortComparator.FieldSort;
import io.activej.dataflow.codec.Subtype;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;

public final class ComparatorCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<RecordSortComparator> recordSortComparator() {
		return StreamCodec.create(RecordSortComparator::new,
				RecordSortComparator::getSorts, StreamCodecs.ofList(
						StreamCodec.create(FieldSort::new,
								FieldSort::index, StreamCodecs.ofVarInt(),
								FieldSort::asc, StreamCodecs.ofBoolean(),
								FieldSort::nullDirection, StreamCodecs.ofEnum(NullDirection.class)
						)
				)
		);
	}

	@Provides
	@Subtype(1)
	StreamCodec<RecordKeyComparator> recordKeyComparator() {
		return StreamCodecs.singleton(RecordKeyComparator.getInstance());
	}
}
