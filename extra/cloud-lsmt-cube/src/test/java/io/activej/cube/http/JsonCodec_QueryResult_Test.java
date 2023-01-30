package io.activej.cube.http;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.QueryResult;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.activej.aggregation.util.Utils.fromJson;
import static io.activej.aggregation.util.Utils.toJson;
import static io.activej.cube.ReportType.DATA_WITH_TOTALS;
import static org.junit.Assert.assertEquals;

public class JsonCodec_QueryResult_Test {

	@Test
	public void test() throws MalformedDataException {
		JsonCodec_QueryResult codec = JsonCodec_QueryResult.create(DefiningClassLoader.create(),
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

		QueryResult queryResult = QueryResult.create(recordScheme,
				List.of("campaign", "site"),
				List.of("clicks", "impressions"),
				List.of("campaign", "clicks"),
				List.of(record1, record2),
				totals,
				123,
				Map.of(
						"campaign", 555,
						"site", "filtered"),
				DATA_WITH_TOTALS
		);

		String json = toJson(codec, queryResult);
		QueryResult decoded = fromJson(codec, json);

		assertEquals(queryResult.getRecordScheme(), decoded.getRecordScheme());
		assertEquals(queryResult.getAttributes(), decoded.getAttributes());
		assertEquals(queryResult.getMeasures(), decoded.getMeasures());
		assertEquals(queryResult.getMeasures(), decoded.getMeasures());
		assertRecords(queryResult.getRecords(), decoded.getRecords());
		assertRecord(queryResult.getTotals(), decoded.getTotals());
		assertEquals(queryResult.getTotalCount(), decoded.getTotalCount());
		assertEquals(queryResult.getFilterAttributes(), decoded.getFilterAttributes());
		assertEquals(queryResult.getSortedBy(), decoded.getSortedBy());
		assertEquals(queryResult.getReportType(), decoded.getReportType());
	}

	private void assertRecords(List<Record> expected, List<Record> actual) {
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertRecord(expected.get(i), actual.get(i));
		}
	}

	private void assertRecord(Record expected, Record actual) {
		assertEquals(expected.getScheme(), actual.getScheme());
		for (int i = 0; i < expected.getScheme().size(); i++) {
			Object expectedValue = expected.get(i);
			Object actualValue = actual.get(i);
			assertEquals(expectedValue, actualValue);
		}
	}
}
