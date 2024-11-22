package io.activej.dataflow.calcite;

import io.activej.dataflow.calcite.dataset.SupplierOfPredicateDataset;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.impl.*;
import io.activej.dataflow.exception.DataflowException;
import io.activej.record.Record;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assume;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static io.activej.common.collection.CollectionUtils.first;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class SqlDataflowTest extends CalciteTestBase {

	private SqlDataflow sqlDataflow;

	@Override
	protected void onSetUp() {
		Assume.assumeTrue(super.sqlDataflow instanceof SqlDataflow);
		sqlDataflow = (SqlDataflow) super.sqlDataflow;
	}

	@Test
	public void countWithoutGrouping() throws SqlParseException, DataflowException {
		Dataset<Record> dataset = sqlDataflow.convertToDataset("""
			SELECT COUNT(*)
			FROM student
			""");

		assertLinear(dataset,
			SupplierOfPredicateDataset.class,	// Initial supplier
			Map.class,						  	// Map Student to Record(int id, String firstName, String lastName, int dept)
			AlreadySorted.class,			  	// Casting to sorted
			LocalSortReduce.class,			  	// Reducing locally to accumulator Record(long COUNT(*))
			RepartitionReduce.class);		  	// Repartitioning and reducing accumulators to output Record(long COUNT(*))
	}

	@Test
	public void countWithGrouping() throws SqlParseException, DataflowException {
		Dataset<Record> dataset = sqlDataflow.convertToDataset("""
			SELECT COUNT(*)
			FROM student
			GROUP BY firstName
			""");

		System.out.println(dataset.toGraphViz());

		assertLinear(dataset,
			SupplierOfPredicateDataset.class,	// Initial supplier
			Map.class,							// Map Student to Record(int id, String firstName, String lastName, int dept)
			Map.class,							// Map Record to Record(String firstName)
			LocalSort.class,					// Sorting locally
			LocalSortReduce.class,				// Reducing locally to accumulator Record(String firstName, long COUNT(*))
			RepartitionReduce.class,			// Repartitioning and reducing accumulators to output Record(String firstName, long COUNT(*))
			Map.class);							// Map output to Record(long COUNT(*))
	}

	@SafeVarargs
	@SuppressWarnings("rawtypes")
	private static void assertLinear(Dataset<Record> dataset, Class<? extends Dataset>... structure) {
		Collections.reverse(Arrays.asList(structure));

		Dataset<?> current = dataset;
		for (int i = 0; i < structure.length; i++) {
			Class<? extends Dataset> datasetClass = structure[i];
			assertThat(current, instanceOf(datasetClass));

			Collection<Dataset<?>> bases = current.getBases();

			if (i == structure.length - 1) {
				assertEquals(0, bases.size());
				return;
			}

			assertEquals(1, bases.size());
			current = first(bases);
		}
	}
}
