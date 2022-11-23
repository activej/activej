package io.activej.dataflow.calcite;

import io.activej.datastream.StreamConsumerToList;
import io.activej.record.Record;
import org.junit.Assume;

import java.io.IOException;
import java.util.List;

import static io.activej.promise.TestUtils.await;

public class CalcitePlainTest extends AbstractCalciteTest {
	@Override
	protected void onSetUp() throws IOException {
		server1.listen();
		server2.listen();
	}

	@Override
	protected QueryResult query(String sql) {
		StreamConsumerToList<Record> resultConsumer = StreamConsumerToList.create();

		List<Record> records = await(sqlDataflow.query(sql)
				.then(supplier -> supplier.streamTo(resultConsumer))
				.whenComplete(server1::close)
				.whenComplete(server2::close)
				.map($ -> resultConsumer.getList()));

		return toQueryResult(records);
	}

	@Override
	protected QueryResult queryPrepared(String sql, ParamsSetter paramsSetter) {
		return denyPreparedRequests();
	}

	@Override
	protected List<QueryResult> queryPreparedRepeated(String sql, ParamsSetter... paramsSetters) {
		return denyPreparedRequests();
	}

	private static QueryResult toQueryResult(List<Record> records) {
		if (records.isEmpty()) return QueryResult.empty();

		return new QueryResult(
				records.get(0).getScheme().getFields(),
				records.stream()
						.map(Record::toArray)
						.toList());
	}

	private <T> T denyPreparedRequests() {
		server1.close();
		server2.close();
		//noinspection ConstantConditions
		Assume.assumeTrue("Prepared statements are not supported in plain queries", false);

		throw new AssertionError();
	}
}
