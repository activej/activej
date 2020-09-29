package io.activej.common.record;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.intersection;
import static java.util.Arrays.asList;

public final class RecordProjection implements BiConsumer<Record, Record>, Function<Record, Record> {
	private final int[] remapObjects;
	private final int[] remapInts;
	private final int[] remapBytes;
	private final int[] remapShorts;
	private final RecordScheme schemeTo;

	private RecordProjection(RecordScheme schemeFrom, RecordScheme schemeTo, Set<String> fields) {
		checkArgument(fields.stream().allMatch(field -> schemeFrom.fieldTypes.get(field) == schemeTo.fieldTypes.get(field)));
		this.schemeTo = schemeTo;
		this.remapObjects = remap(schemeFrom, schemeTo, fields, RecordScheme.INTERNAL_OBJECT);
		this.remapInts = remapInts(schemeFrom, schemeTo, fields);
		this.remapBytes = remap(schemeFrom, schemeTo, fields, RecordScheme.INTERNAL_BOOLEAN, RecordScheme.INTERNAL_BYTE);
		this.remapShorts = remap(schemeFrom, schemeTo, fields, RecordScheme.INTERNAL_SHORT, RecordScheme.INTERNAL_CHAR);
	}

	private static int[] remap(RecordScheme schemeFrom, RecordScheme schemeTo, Set<String> fields, int... internalTypes) {
		Arrays.sort(internalTypes);
		return fields.stream()
				.filter(field -> Arrays.binarySearch(internalTypes, RecordScheme.unpackType(schemeFrom.fieldRawIndices.get(field))) >= 0)
				.mapToInt(field -> pack(schemeFrom, schemeTo, field))
				.sorted()
				.toArray();
	}

	private static int[] remapInts(RecordScheme schemeFrom, RecordScheme schemeTo, Set<String> fields) {
		return IntStream.concat(
				fields.stream()
						.filter(field -> {
							int type = RecordScheme.unpackType(schemeFrom.fieldRawIndices.get(field));
							return type == RecordScheme.INTERNAL_LONG || type == RecordScheme.INTERNAL_DOUBLE;
						})
						.flatMapToInt(field -> IntStream.of(
								pack(schemeFrom, schemeTo, field),
								pack2(schemeFrom, schemeTo, field)
						)),
				fields.stream()
						.filter(field -> {
							int type = RecordScheme.unpackType(schemeFrom.fieldRawIndices.get(field));
							return type == RecordScheme.INTERNAL_INT || type == RecordScheme.INTERNAL_FLOAT;
						})
						.mapToInt(field -> pack(schemeFrom, schemeTo, field)))
				.sorted()
				.toArray();
	}

	private static int pack(RecordScheme schemeFrom, RecordScheme schemeTo, String field) {
		return (RecordScheme.unpackIndex(schemeFrom.fieldRawIndices.get(field)) << 16) | RecordScheme.unpackIndex(schemeTo.fieldRawIndices.get(field));
	}

	private static int pack2(RecordScheme schemeFrom, RecordScheme schemeTo, String field) {
		return ((RecordScheme.unpackIndex(schemeFrom.fieldRawIndices.get(field)) + 1) << 16) | (RecordScheme.unpackIndex(schemeTo.fieldRawIndices.get(field)) + 1);
	}

	public static RecordProjection create(RecordScheme schemeFrom, String... fields) {
		return create(schemeFrom, asList(fields));
	}

	public static RecordProjection create(RecordScheme schemeFrom, Collection<String> fields) {
		RecordScheme schemeTo = RecordScheme.create();
		for (String field : fields) {
			schemeTo.addField(field, schemeFrom.getFieldType(field));
		}
		return new RecordProjection(schemeFrom, schemeTo, new HashSet<>(fields));
	}

	public static RecordProjection create(RecordScheme schemeFrom, RecordScheme schemeTo) {
		return create(schemeFrom, schemeTo, intersection(schemeFrom.fieldIndices.keySet(), schemeTo.fieldIndices.keySet()));
	}

	public static RecordProjection create(RecordScheme schemeFrom, RecordScheme schemeTo, Set<String> fields) {
		return new RecordProjection(schemeFrom, schemeTo, fields);
	}

	@SuppressWarnings("ForLoopReplaceableByForEach")
	@Override
	public void accept(Record recordFrom, Record recordTo) {
		for (int i = 0; i < remapObjects.length; i++) {
			recordTo.objects[remapObjects[i] & 0xFFFF] = recordFrom.objects[remapObjects[i] >>> 16];
		}
		for (int i = 0; i < remapInts.length; i++) {
			recordTo.ints[remapInts[i] & 0xFFFF] = recordFrom.ints[remapInts[i] >>> 16];
		}
		for (int i = 0; i < remapBytes.length; i++) {
			int idxFrom = remapBytes[i] >>> 16;
			int idxTo = remapBytes[i] & 0xFFFF;
			int posFrom = idxFrom >>> 2;
			int posTo = idxTo >>> 2;
			int offsetFrom = (idxFrom & 3) * 8;
			int offsetTo = (idxTo & 3) * 8;
			recordTo.ints[posTo] += (recordFrom.ints[posFrom] >>> offsetFrom & 0xFF) << offsetTo;
		}
		for (int i = 0; i < remapShorts.length; i++) {
			int idxFrom = remapShorts[i] >>> 16;
			int idxTo = remapShorts[i] & 0xFFFF;
			int posFrom = idxFrom >>> 2;
			int posTo = idxTo >>> 2;
			int offsetFrom = (idxFrom & 3) * 8;
			int offsetTo = (idxTo & 3) * 8;
			recordTo.ints[posTo] += (recordFrom.ints[posFrom] >>> offsetFrom & 0xFFFF) << offsetTo;
		}
	}

	@Override
	public Record apply(Record recordFrom) {
		Record recordTo = Record.create(schemeTo);
		accept(recordFrom, recordTo);
		return recordTo;
	}

}
