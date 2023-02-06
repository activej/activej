package io.activej.dataflow.calcite;

import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.datastream.supplier.StreamSupplier;

public interface FilteredDataflowSupplier<T> {
	StreamSupplier<T> create(WherePredicate predicate);
}
