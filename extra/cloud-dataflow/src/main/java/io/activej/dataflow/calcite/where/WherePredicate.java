package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.SerializeClass;

import java.util.function.Predicate;

@SerializeClass(subclasses = {
		AndPredicate.class,
		OrPredicate.class,
		EqPredicate.class,
		NotEqPredicate.class,
		GePredicate.class,
		GtPredicate.class,
		LePredicate.class,
		LtPredicate.class,
		BetweenPredicate.class,
		InPredicate.class,
		LikePredicate.class
})
public interface WherePredicate extends Predicate<Record> {
}
