package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.aggregation.*;
import io.activej.dataflow.proto.calcite.RecordSchemeProto;
import io.activej.dataflow.proto.calcite.ReducerProto;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.util.ArrayList;
import java.util.List;

import static io.activej.serializer.BinarySerializers.BYTES_SERIALIZER;


public final class ReducerSerializer implements BinarySerializer<RecordReducer> {
	private final DefiningClassLoader classLoader;

	public ReducerSerializer(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public ReducerSerializer() {
		this(DefiningClassLoader.create());
	}

	@Override
	public void encode(BinaryOutput out, RecordReducer recordReducer) {
		BYTES_SERIALIZER.encode(out, convert(recordReducer).toByteArray());
	}

	@Override
	public RecordReducer decode(BinaryInput in) throws CorruptedDataException {
		byte[] serialized = BYTES_SERIALIZER.decode(in);

		ReducerProto.RecordReducer predicate;
		try {
			predicate = ReducerProto.RecordReducer.parseFrom(serialized);
		} catch (InvalidProtocolBufferException e) {
			throw new CorruptedDataException(e.getMessage());
		}

		return convert(classLoader, predicate);
	}

	private static ReducerProto.RecordReducer convert(RecordReducer recordReducer) {
		return ReducerProto.RecordReducer.newBuilder()
				.setRecordScheme(RecordSchemeConverter.convert(recordReducer.getOriginalScheme()))
				.addAllFieldReducers(recordReducer.getReducers().stream()
						.map(ReducerSerializer::convert)
						.toList())
				.build();
	}

	private static ReducerProto.RecordReducer.FieldReducer convert(FieldReducer<?, ?, ?> fieldReducer) {
		ReducerProto.RecordReducer.FieldReducer.Builder builder = ReducerProto.RecordReducer.FieldReducer.newBuilder();
		int fieldIndex = fieldReducer.getFieldIndex();
		String fieldAlias = fieldReducer.getFieldAlias();

		ReducerProto.RecordReducer.FieldAlias.Builder fieldAliasBuilder = ReducerProto.RecordReducer.FieldAlias.newBuilder();
		if (fieldAlias == null) {
			fieldAliasBuilder.setIsNull(true);
		} else {
			fieldAliasBuilder.setAlias(fieldAlias);
		}

		if (fieldReducer instanceof KeyReducer<?>) {
			builder.setKeyReducer(ReducerProto.RecordReducer.FieldReducer.KeyReducer.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else if (fieldReducer instanceof CountReducer<?>) {
			builder.setCountReducer(ReducerProto.RecordReducer.FieldReducer.CountReducer.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else if (fieldReducer instanceof SumReducerInteger<?>) {
			builder.setSumReducerInteger(ReducerProto.RecordReducer.FieldReducer.SumReducerInteger.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else if (fieldReducer instanceof SumReducerDecimal<?>) {
			builder.setSumReducerDecimal(ReducerProto.RecordReducer.FieldReducer.SumReducerDecimal.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else if (fieldReducer instanceof AvgReducer) {
			builder.setAvgReducer(ReducerProto.RecordReducer.FieldReducer.AvgReducer.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else if (fieldReducer instanceof MinReducer) {
			builder.setMinReducer(ReducerProto.RecordReducer.FieldReducer.MinReducer.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else if (fieldReducer instanceof MaxReducer) {
			builder.setMaxReducer(ReducerProto.RecordReducer.FieldReducer.MaxReducer.newBuilder()
					.setFieldIndex(fieldIndex)
					.setFieldAlias(fieldAliasBuilder));
		} else {
			throw new IllegalArgumentException("Unknown field reducer type: " + fieldReducer.getClass());
		}

		return builder.build();
	}

	private static RecordReducer convert(DefiningClassLoader classLoader, ReducerProto.RecordReducer recordReducer) {
		RecordSchemeProto.RecordScheme recordScheme = recordReducer.getRecordScheme();
		List<FieldReducer<?, ?, ?>> fieldReducers = new ArrayList<>();
		for (ReducerProto.RecordReducer.FieldReducer x : recordReducer.getFieldReducersList()) {
			fieldReducers.add(convert(x));
		}
		return RecordReducer.create(RecordSchemeConverter.convert(classLoader, recordScheme), fieldReducers);
	}

	private static FieldReducer<?, ?, ?> convert(ReducerProto.RecordReducer.FieldReducer fieldReducer) {
		return switch (fieldReducer.getFieldReducerCase()) {
			case KEY_REDUCER -> {
				ReducerProto.RecordReducer.FieldReducer.KeyReducer keyReducer = fieldReducer.getKeyReducer();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = keyReducer.getFieldAlias();
				yield new KeyReducer<>(keyReducer.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case COUNT_REDUCER -> {
				ReducerProto.RecordReducer.FieldReducer.CountReducer countReducer = fieldReducer.getCountReducer();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = countReducer.getFieldAlias();
				yield new CountReducer<>(countReducer.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case SUM_REDUCER_INTEGER -> {
				ReducerProto.RecordReducer.FieldReducer.SumReducerInteger sumReducerInteger = fieldReducer.getSumReducerInteger();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = sumReducerInteger.getFieldAlias();
				yield new SumReducerInteger<>(sumReducerInteger.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case SUM_REDUCER_DECIMAL -> {
				ReducerProto.RecordReducer.FieldReducer.SumReducerDecimal sumReducerDecimal = fieldReducer.getSumReducerDecimal();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = sumReducerDecimal.getFieldAlias();
				yield new SumReducerDecimal<>(sumReducerDecimal.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case AVG_REDUCER -> {
				ReducerProto.RecordReducer.FieldReducer.AvgReducer avgReducer = fieldReducer.getAvgReducer();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = avgReducer.getFieldAlias();
				yield new AvgReducer(avgReducer.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case MIN_REDUCER -> {
				ReducerProto.RecordReducer.FieldReducer.MinReducer minReducer = fieldReducer.getMinReducer();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = minReducer.getFieldAlias();
				yield new MinReducer<>(minReducer.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case MAX_REDUCER -> {
				ReducerProto.RecordReducer.FieldReducer.MaxReducer maxReducer = fieldReducer.getMaxReducer();
				ReducerProto.RecordReducer.FieldAlias fieldAlias = maxReducer.getFieldAlias();
				yield new MaxReducer<>(maxReducer.getFieldIndex(), fieldAlias.hasIsNull() ? null : fieldAlias.getAlias());
			}
			case FIELDREDUCER_NOT_SET -> throw new CorruptedDataException("Field predicate was not set");
		};
	}

}
