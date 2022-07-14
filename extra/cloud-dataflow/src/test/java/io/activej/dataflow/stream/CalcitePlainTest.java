package io.activej.dataflow.stream;

import io.activej.datastream.StreamConsumerToList;
import io.activej.record.Record;
import org.junit.Assume;

import java.io.IOException;
import java.util.List;

import static io.activej.promise.TestUtils.await;

public class CalcitePlainTest extends AbstractCalciteTest {
	@Override
	protected void onSetUp() throws IOException {
		server.listen();
	}

	@Override
	protected QueryResult query(String sql) {
		StreamConsumerToList<Record> resultConsumer = StreamConsumerToList.create();

		List<Record> records = await(sqlDataflow.query(sql)
				.then(supplier -> supplier.streamTo(resultConsumer))
				.whenComplete(server::close)
				.map($ -> resultConsumer.getList()));

		return toQueryResult(records);
	}

	@Override
	protected QueryResult queryPrepared(String sql, ParamsSetter paramsSetter) {
		server.close();
		//noinspection ConstantConditions
		Assume.assumeTrue("Prepared statements are not supported in plain calls", false);

		throw new AssertionError();
	}

	private static QueryResult toQueryResult(List<Record> records) {
		if (records.isEmpty()) return QueryResult.empty();

		return new QueryResult(
				records.get(0).getScheme().getFields(),
				records.stream()
						.map(Record::toArray)
						.toList());
	}
}
