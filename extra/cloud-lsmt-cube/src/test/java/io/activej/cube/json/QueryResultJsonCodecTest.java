package io.activej.cube.json;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.QueryResult;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecFactory;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.activej.json.JsonUtils.fromJson;
import static io.activej.json.JsonUtils.toJson;
import static org.junit.Assert.assertEquals;

public class QueryResultJsonCodecTest {

	@Test
	public void testForDataWithTotals() throws MalformedDataException {
		JsonCodec<QueryResult> codec = JsonCodecs.createQueryResultCodec(DefiningClassLoader.create(),
			JsonCodecFactory.defaultInstance(),
			Map.of(
				"campaign", int.class,
				"site", String.class),
			Map.of(
				"impressions", long.class,
				"clicks", long.class));

		RecordScheme recordScheme = RecordScheme.builder()
			.withField("campaign", int.class)
			.withField("site", String.class)
			.withField("clicks", long.class)
			.withField("impressions", long.class)
			.build();

		Record record1 = recordScheme.record();
		record1.set("campaign", 123);
		record1.set("site", "Test");
		record1.set("clicks", 12345L);
		record1.set("impressions", 55352L);

		Record record2 = recordScheme.record();
		record2.set("campaign", -123);
		record2.set("site", null);
		record2.set("clicks", 1412L);
		record2.set("impressions", 3523L);

		Record totals = recordScheme.record();
		totals.set("clicks", 5555L);
		totals.set("impressions", 66756L);

		QueryResult queryResult = QueryResult.createForDataWithTotals(recordScheme,
			List.of("campaign", "site"), List.of("clicks", "impressions"),
			List.of("campaign", "clicks"), Map.of("campaign", 555, "site", "filtered"),
			List.of(record1, record2), totals, 123
		);

		String json = toJson(codec, queryResult);
		QueryResult decoded = fromJson(codec, json);

		assertResult(queryResult, decoded);
	}

	@Test
	public void testForDataWithZeroTotals() throws MalformedDataException {
		JsonCodec<QueryResult> codec = JsonCodecs.createQueryResultCodec(DefiningClassLoader.create(),
			JsonCodecFactory.defaultInstance(),
			Map.of(
				"campaign", int.class,
				"site", String.class),
			Map.of(
				"impressions", long.class,
				"clicks", long.class));

		RecordScheme recordScheme = RecordScheme.builder()
			.withField("campaign", int.class)
			.withField("site", String.class)
			.withField("clicks", long.class)
			.withField("impressions", long.class)
			.build();

		Record totals = recordScheme.record();
		totals.set("clicks", 0L);
		totals.set("impressions", 0L);

		QueryResult queryResult = QueryResult.createForDataWithTotals(recordScheme,
			List.of("campaign", "site"), List.of("clicks", "impressions"),
			List.of("campaign", "clicks"), Map.of("campaign", 555, "site", "filtered"),
			List.of(), totals, 0
		);

		String json = toJson(codec, queryResult);
		QueryResult decoded = fromJson(codec, json);

		assertResult(queryResult, decoded);
	}

	@Test
	public void testForData() throws MalformedDataException {
		JsonCodec<QueryResult> codec = JsonCodecs.createQueryResultCodec(DefiningClassLoader.create(),
			JsonCodecFactory.defaultInstance(),
			Map.of(
				"campaign", int.class,
				"site", String.class),
			Map.of(
				"impressions", long.class,
				"clicks", long.class));

		RecordScheme recordScheme = RecordScheme.builder()
			.withField("campaign", int.class)
			.withField("site", String.class)
			.withField("clicks", long.class)
			.withField("impressions", long.class)
			.build();

		Record record1 = recordScheme.record();
		record1.set("campaign", 123);
		record1.set("site", "Test");
		record1.set("clicks", 12345L);
		record1.set("impressions", 55352L);

		Record record2 = recordScheme.record();
		record2.set("campaign", -123);
		record2.set("site", null);
		record2.set("clicks", 1412L);
		record2.set("impressions", 3523L);

		QueryResult queryResult = QueryResult.createForData(recordScheme,
			List.of("campaign", "site"), List.of("clicks", "impressions"),
			List.of("campaign", "clicks"), Map.of("campaign", 555, "site", "filtered"),
			List.of(record1, record2)
		);

		String json = toJson(codec, queryResult);
		QueryResult decoded = fromJson(codec, json);

		assertResult(queryResult, decoded);
	}

	@Test
	public void testForZeroData() throws MalformedDataException {
		JsonCodec<QueryResult> codec = JsonCodecs.createQueryResultCodec(DefiningClassLoader.create(),
			JsonCodecFactory.defaultInstance(),
			Map.of(
				"campaign", int.class,
				"site", String.class),
			Map.of(
				"impressions", long.class,
				"clicks", long.class));

		RecordScheme recordScheme = RecordScheme.builder()
			.withField("campaign", int.class)
			.withField("site", String.class)
			.withField("clicks", long.class)
			.withField("impressions", long.class)
			.build();

		QueryResult queryResult = QueryResult.createForData(recordScheme,
			List.of("campaign", "site"), List.of("clicks", "impressions"),
			List.of("campaign", "clicks"), Map.of("campaign", 555, "site", "filtered"),
			List.of()
		);

		String json = toJson(codec, queryResult);
		QueryResult decoded = fromJson(codec, json);

		assertResult(queryResult, decoded);
	}

	@Test
	public void testForMetadata() throws MalformedDataException {
		JsonCodec<QueryResult> codec = JsonCodecs.createQueryResultCodec(DefiningClassLoader.create(),
			JsonCodecFactory.defaultInstance(),
			Map.of(
				"campaign", int.class,
				"site", String.class),
			Map.of(
				"impressions", long.class,
				"clicks", long.class));

		RecordScheme recordScheme = RecordScheme.builder()
			.withField("campaign", int.class)
			.withField("site", String.class)
			.withField("clicks", long.class)
			.withField("impressions", long.class)
			.build();

		QueryResult queryResult = QueryResult.createForMetadata(recordScheme,
			List.of("campaign", "site"), List.of("clicks", "impressions")
		);

		String json = toJson(codec, queryResult);
		QueryResult decoded = fromJson(codec, json);

		assertResult(queryResult, decoded);
	}

	private static void assertResult(QueryResult expected, QueryResult actual) {
		assertEquals(expected.getRecordScheme(), actual.getRecordScheme());
		assertEquals(expected.getAttributes(), actual.getAttributes());
		assertEquals(expected.getMeasures(), actual.getMeasures());
		assertEquals(expected.getMeasures(), actual.getMeasures());
		assertRecords(expected.getRecords(), actual.getRecords());
		assertRecord(expected.getTotals(), actual.getTotals());
		assertEquals(expected.getTotalCount(), actual.getTotalCount());
		assertEquals(expected.getFilterAttributes(), actual.getFilterAttributes());
		assertEquals(expected.getSortedBy(), actual.getSortedBy());
		assertEquals(expected.getReportType(), actual.getReportType());
	}

	private static void assertRecords(List<Record> expected, List<Record> actual) {
		if (expected == actual) return;
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertRecord(expected.get(i), actual.get(i));
		}
	}

	private static void assertRecord(Record expected, Record actual) {
		if (expected == actual) return;
		assertEquals(expected.getScheme(), actual.getScheme());
		for (int i = 0; i < expected.getScheme().size(); i++) {
			Object expectedValue = expected.get(i);
			Object actualValue = actual.get(i);
			assertEquals(expectedValue, actualValue);
		}
	}
}
