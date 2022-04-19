package io.activej.aggregation;

import io.activej.aggregation.fieldtype.FieldType;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static io.activej.aggregation.AggregationPredicates.*;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.codegen.expression.Expressions.arg;
import static io.activej.codegen.expression.Expressions.cast;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class PredicatesTest {
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Test
	public void testSimplify() {
		assertEquals(alwaysFalse(), and(eq("publisher", 10), eq("publisher", 20)).simplify());
		assertEquals(eq("publisher", 10), and(eq("publisher", 10), not(not(eq("publisher", 10)))).simplify());
		assertEquals(eq("publisher", 20), and(alwaysTrue(), eq("publisher", 20)).simplify());
		assertEquals(alwaysFalse(), and(alwaysFalse(), eq("publisher", 20)).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)), and(eq("date", 20160101), eq("publisher", 20)).simplify());

		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(not(not(and(not(not(eq("date", 20160101))), eq("publisher", 20)))), not(not(eq("publisher", 20)))).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(and(not(not(eq("publisher", 20))), not(not(eq("date", 20160101)))), and(eq("date", 20160101), eq("publisher", 20))).simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateEqSimplification() {
		AggregationPredicate expected = eq("x", 10);
		AggregationPredicate actual = and(notEq("x", 12), eq("x", 10));
		AggregationPredicate actual2 = and(eq("x", 10), notEq("x", 12));
		assertEquals(expected, actual.simplify());
		// test symmetry
		assertEquals(expected, actual2.simplify());
	}

	@Test
	public void testPredicateNot_NegatesPredicateEqAndPredicateNotEqProperly() {
		assertEquals(eq("x", 10), not(notEq("x", 10)).simplify());
		assertEquals(notEq("x", 10), not(eq("x", 10)).simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateEq() {
		assertEquals(alwaysFalse(), and(eq("x", 10), notEq("x", 10)).simplify());
		assertEquals(alwaysFalse(), and(notEq("x", 10), eq("x", 10)).simplify());
	}

	@Test
	public void testUnnecessaryPredicates_areRemoved_whenSimplified() {
		AggregationPredicate predicate = and(
				not(eq("x", 1)),
				notEq("x", 1),
				not(not(not(eq("x", 1)))),
				eq("x", 2));
		AggregationPredicate expected = and(notEq("x", 1), eq("x", 2)).simplify();
		assertEquals(expected, predicate.simplify());
	}

	enum TestEnum {ONE, TWO, THREE}

	@Test
	public void testBetweenPredicateAndPredicateNotEq() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", 6), between("x", 5, 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", 5), between("x", 5, 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", 10), between("x", 5, 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", 12), between("x", 5, 10));
		assertEquals(between("x", 5, 10), predicate.simplify());

		predicate = and(has("TestEnum"), between("TestEnum", TestEnum.ONE, TestEnum.THREE));
		assertEquals(between("TestEnum", TestEnum.ONE, TestEnum.THREE), predicate.simplify());
	}

	@Test
	public void testPredicateGtAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(ge("x", 10), gt("x", 10));
		assertEquals(gt("x", 10), predicate.simplify());

		predicate = and(gt("x", 10), ge("x", 10));
		assertEquals(gt("x", 10), predicate.simplify());

		predicate = and(gt("x", 11), ge("x", 10));
		assertEquals(gt("x", 11), predicate.simplify());

		predicate = and(ge("x", 11), gt("x", 10));
		assertEquals(ge("x", 11), predicate.simplify());

		predicate = and(ge("x", 10), gt("x", 11));
		assertEquals(gt("x", 11), predicate.simplify());
	}

	@Test
	public void testPredicateGeAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(ge("x", 10), ge("x", 11));
		assertEquals(ge("x", 11), predicate.simplify());

		predicate = and(ge("x", 11), ge("x", 10));
		assertEquals(ge("x", 11), predicate.simplify());

		predicate = and(ge("x", 10), ge("x", 10));
		assertEquals(ge("x", 10), predicate.simplify());
	}

	@Test
	public void testPredicateGtAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(gt("x", 10), gt("x", 11));
		assertEquals(gt("x", 11), predicate.simplify());

		predicate = and(gt("x", 11), gt("x", 10));
		assertEquals(gt("x", 11), predicate.simplify());

		predicate = and(gt("x", 10), gt("x", 10));
		assertEquals(gt("x", 10), predicate.simplify());
	}

	@Test
	public void testPredicateLeAndPredicateLe() {
		AggregationPredicate predicate;
		predicate = and(le("x", 10), le("x", 11));
		assertEquals(le("x", 10), predicate.simplify());

		predicate = and(le("x", 11), le("x", 10));
		assertEquals(le("x", 10), predicate.simplify());

		predicate = and(le("x", 10), le("x", 10));
		assertEquals(le("x", 10), predicate.simplify());
	}

	@Test
	public void testPredicateLeAndPredicateLt() {
		AggregationPredicate predicate;
		predicate = and(le("x", 11), lt("x", 11));
		assertEquals(lt("x", 11), predicate.simplify());

		predicate = and(le("x", 11), lt("x", 10));
		assertEquals(lt("x", 10), predicate.simplify());

		predicate = and(le("x", 10), lt("x", 11));
		assertEquals(le("x", 10), predicate.simplify());
	}

	@Test
	public void testPredicateGeAndPredicateLe() {
		AggregationPredicate predicate;
		predicate = and(ge("x", 11), le("x", 11));
		assertEquals(eq("x", 11), predicate.simplify());

		predicate = and(ge("x", 11), le("x", 10));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(ge("x", 10), le("x", 11));
		assertEquals(between("x", 10, 11), predicate.simplify());
	}

	@Test
	public void testPredicateLtAndPredicateLt() {
		AggregationPredicate predicate;
		predicate = and(lt("x", 11), lt("x", 11));
		assertEquals(lt("x", 11), predicate.simplify());

		predicate = and(lt("x", 11), lt("x", 10));
		assertEquals(lt("x", 10), predicate.simplify());

		predicate = and(lt("x", 10), lt("x", 11));
		assertEquals(lt("x", 10), predicate.simplify());
	}

	@Test
	public void testPredicateLtAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(lt("x", 11), ge("x", 11));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(lt("x", 10), ge("x", 11));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(lt("x", 11), ge("x", 10));
		assertEquals(predicate, predicate.simplify());
	}

	@Test
	public void testPredicateLeAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(le("x", 11), gt("x", 11));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(le("x", 11), gt("x", 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(le("x", 10), gt("x", 11));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateLtAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(lt("x", 11), gt("x", 11));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(lt("x", 11), gt("x", 10));
		assertEquals(predicate, predicate.simplify());

		predicate = and(lt("x", 10), gt("x", 11));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateBetweenAndPredicateLe() {
		AggregationPredicate predicate;
		predicate = and(between("x", -5, 5), le("x", -6));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(between("x", -5, 5), le("x", -5));
		assertEquals(eq("x", -5), predicate.simplify());

		predicate = and(between("x", -5, 5), le("x", 0));
		assertEquals(between("x", -5, 0), predicate.simplify());

		predicate = and(between("x", -5, 5), le("x", 5));
		assertEquals(between("x", -5, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), le("x", 6));
		assertEquals(between("x", -5, 5), predicate.simplify());
	}

	@Test
	public void testPredicateBetweenAndPredicateLt() {
		AggregationPredicate predicate;
		predicate = and(between("x", -5, 5), lt("x", -6));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(between("x", -5, 5), lt("x", -5));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(between("x", -5, 5), lt("x", 0));
		assertEquals(predicate, predicate.simplify());

		predicate = and(between("x", -5, 5), lt("x", 5));
		assertEquals(predicate, predicate.simplify());

		predicate = and(between("x", -5, 5), lt("x", 6));
		assertEquals(between("x", -5, 5), predicate.simplify());
	}

	@Test
	public void testPredicateBetweenAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(between("x", -5, 5), ge("x", -6));
		assertEquals(between("x", -5, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), ge("x", -5));
		assertEquals(between("x", -5, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), ge("x", 0));
		assertEquals(between("x", 0, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), ge("x", 5));
		assertEquals(eq("x", 5), predicate.simplify());

		predicate = and(between("x", -5, 5), ge("x", 6));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateBetweenAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(between("x", -5, 5), gt("x", -6));
		assertEquals(between("x", -5, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), gt("x", -5));
		assertEquals(predicate, predicate.simplify());

		predicate = and(between("x", -5, 5), gt("x", 0));
		assertEquals(predicate, predicate.simplify());

		predicate = and(between("x", -5, 5), gt("x", 5));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());

		predicate = and(between("x", -5, 5), gt("x", 6));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateBetweenAndPredicateBetween() {
		AggregationPredicate predicate;
		predicate = and(between("x", -5, 5), between("x", 5, 10));
		assertEquals(eq("x", 5), predicate.simplify());

		predicate = and(between("x", -5, 5), between("x", 4, 6));
		assertEquals(between("x", 4, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), between("x", -6, 6));
		assertEquals(between("x", -5, 5), predicate.simplify());

		predicate = and(between("x", -5, 5), between("x", 6, 7));
		assertEquals(AggregationPredicates.alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateLe() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", -5), le("x", -4));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", -5), le("x", -5));
		assertEquals(lt("x", -5), predicate.simplify());

		predicate = and(notEq("x", -5), le("x", -6));
		assertEquals(le("x", -6), predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateLt() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", -5), lt("x", -4));
		assertEquals(predicate, predicate.simplify());

		predicate = and(notEq("x", -5), lt("x", -5));
		assertEquals(lt("x", -5), predicate.simplify());

		predicate = and(notEq("x", -5), lt("x", -6));
		assertEquals(lt("x", -6), predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", -5), ge("x", -4));
		assertEquals(ge("x", -4), predicate.simplify());

		predicate = and(notEq("x", -5), ge("x", -5));
		assertEquals(gt("x", -5), predicate.simplify());

		predicate = and(notEq("x", -5), ge("x", -6));
		assertEquals(predicate, predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", -5), gt("x", -4));
		assertEquals(gt("x", -4), predicate.simplify());

		predicate = and(notEq("x", -5), gt("x", -5));
		assertEquals(gt("x", -5), predicate.simplify());

		predicate = and(notEq("x", -5), gt("x", -6));
		assertEquals(predicate, predicate.simplify());
	}

	@Test
	public void testPredicateEqAndPredicateLe() {
		AggregationPredicate predicate;
		predicate = and(eq("x", -5), le("x", -4));
		assertEquals(eq("x", -5), predicate.simplify());

		predicate = and(eq("x", -5), le("x", -5));
		assertEquals(eq("x", -5), predicate.simplify());

		predicate = and(eq("x", -5), le("x", -6));
		assertEquals(alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateEqAndPredicateLt() {
		AggregationPredicate predicate;
		predicate = and(eq("x", -5), lt("x", -4));
		assertEquals(eq("x", -5), predicate.simplify());

		predicate = and(eq("x", -5), lt("x", -5));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(eq("x", -5), lt("x", -6));
		assertEquals(alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateEqAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(eq("x", -5), ge("x", -4));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(eq("x", -5), ge("x", -5));
		assertEquals(eq("x", -5), predicate.simplify());

		predicate = and(eq("x", -5), ge("x", -6));
		assertEquals(eq("x", -5), predicate.simplify());
	}

	@Test
	public void testPredicateEqAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(eq("x", -5), gt("x", -4));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(eq("x", -5), gt("x", -5));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(eq("x", -5), gt("x", -6));
		assertEquals(eq("x", -5), predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateNotEq() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", -5), notEq("x", -5));
		assertEquals(notEq("x", -5), predicate.simplify());

		predicate = and(notEq("x", -5), notEq("x", -6));
		assertEquals(predicate, predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateIn() {
		AggregationPredicate predicate;
		predicate = and(notEq("x", 0), in("x"));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(notEq("x", 0), in("x", 0));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(notEq("x", 0), in("x", 1));
		assertEquals(in("x", 1), predicate.simplify());

		predicate = and(notEq("x", 0), in("x", 0, 1, 2, 3));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(notEq("x", 0), has("x"));
		assertEquals(notEq("x", 0), predicate.simplify());
	}

	@Test
	public void testPredicateNotEqAndPredicateInAndPredicateHas() {
		AggregationPredicate predicate, predicateHas, predicateNotEq, predicateIn;
		predicateHas = has("x");
		predicateNotEq = notEq("x", 0);

		predicateIn = in("x");
		predicate = and(predicateIn, predicateHas);
		assertEquals(alwaysFalse(), predicate.simplify());
		predicate = and(predicateIn, predicateNotEq);
		assertEquals(alwaysFalse(), predicate.simplify());

		predicateIn = in("x", 1, 2, 3);
		predicate = and(predicateIn, predicateHas);
		assertEquals(in("x", 1, 2, 3), predicate.simplify());
		predicate = and(predicateIn, predicateNotEq);
		assertEquals(in("x", 1, 2, 3), predicate.simplify());
	}

	@Test
	public void testPredicateInAndPredicateEq() {
		AggregationPredicate predicate;
		predicate = and(in("x", 1, 2, 3, 5), eq("x", 2));
		assertEquals(eq("x", 2), predicate.simplify());

		predicate = and(in("x", 1, 2, 3, 5), eq("x", 4));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1), eq("x", 1));
		assertEquals(eq("x", 1), predicate.simplify());
	}

	@Test
	public void testPredicateInAndPredicateIn() {
		AggregationPredicate predicate;
		predicate = and(in("x", 1, 2, 3, 5), in("x", 2));
		assertEquals(eq("x", 2), predicate.simplify());

		predicate = and(in("x", 1, 2, 3, 5), in("x", 4));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1, 3, 5, 7), in("x", 2, 4, 6, 8));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1), in("x", 2));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1, 2), in("x", -1, -2));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1), in("x", 1, 2));
		assertEquals(eq("x", 1), predicate.simplify());
	}

	@Test
	public void testPredicateInAndPredicateLe() {
		AggregationPredicate predicate;
		predicate = and(in("x", 1, 2, 3, 5), le("x", 2));
		assertEquals(in("x", 1, 2), predicate.simplify());

		predicate = and(in("x", 1), le("x", 2));
		assertEquals(eq("x", 1), predicate.simplify());

		predicate = and(in("x", 1, 2), le("x", 2));
		assertEquals(in("x", 1, 2), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), le("x", 2));
		assertEquals(in("x", 1, 2), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), le("x", 0));
		assertEquals(alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateInAndPredicateLt() {
		AggregationPredicate predicate;
		predicate = and(in("x", 1, 2, 3, 5), lt("x", 2));
		assertEquals(in("x", 1), predicate.simplify());

		predicate = and(in("x", 1), lt("x", 2));
		assertEquals(eq("x", 1), predicate.simplify());

		predicate = and(in("x", 1, 2), lt("x", 2));
		assertEquals(eq("x", 1), predicate.simplify());

		predicate = and(in("x", 1, 2, 4), lt("x", 3));
		assertEquals(in("x", 1, 2), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), lt("x", 0));
		assertEquals(alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateInAndPredicateGe() {
		AggregationPredicate predicate;
		predicate = and(in("x", 1, 2, 3, 5), ge("x", 2));
		assertEquals(in("x", 2, 3, 5), predicate.simplify());

		predicate = and(in("x", 1), ge("x", 2));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1, 2), ge("x", 2));
		assertEquals(eq("x", 2), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), ge("x", 1));
		assertEquals(in("x", 1, 2, 3), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), ge("x", 0));
		assertEquals(in("x", 1, 2, 3), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), ge("x", 4));
		assertEquals(alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateInAndPredicateGt() {
		AggregationPredicate predicate;
		predicate = and(in("x", 1, 2, 3, 5), gt("x", 2));
		assertEquals(in("x", 3, 5), predicate.simplify());

		predicate = and(in("x", 1), gt("x", 2));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1, 2), gt("x", 2));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(in("x", 1, 2, 3), gt("x", 0));
		assertEquals(in("x", 1, 2, 3), predicate.simplify());
	}

	@Test
	public void testPredicateRegexp() {
		Record record = new Record();
		assertTrue(matches(record, "booleanValue", "^false$"));
		record.booleanValue = true;
		assertTrue(matches(record, "booleanValue", "^true$"));

		assertTrue(matches(record, "byteValue", "^0$"));
		record.byteValue = 123;
		assertTrue(matches(record, "byteValue", "^123$"));

		record.shortValue = 123;
		assertTrue(matches(record, "shortValue", "^123$"));

		record.charValue = 'i';
		assertTrue(matches(record, "charValue", "^i$"));

		record.intValue = -123;
		assertTrue(matches(record, "intValue", "^-123$"));

		record.longValue = Long.MIN_VALUE;
		assertTrue(matches(record, "longValue", "^" + Long.MIN_VALUE + "$"));

		record.floatValue = 0.000000001f;
		assertTrue(matches(record, "floatValue", "^1\\.0E-9$"));

		record.doubleValue = 123.33333;
		assertTrue(matches(record, "doubleValue", "^123\\.3+$"));

		// null is not matched
		assertFalse(matches(record, "stringValue", ".*"));
		record.stringValue = "abcdefg";
		assertTrue(matches(record, "stringValue", "^abc.*fg$"));
	}

	@Test
	public void testPredicateRegexpAndPredicateEq() {
		AggregationPredicate predicate;
		predicate = and(regexp("x", "tes."), eq("x", "test"));
		assertEquals(eq("x", "test"), predicate.simplify());

		predicate = and(regexp("x", ".*"), eq("x", "test"));
		assertEquals(eq("x", "test"), predicate.simplify());

		predicate = and(regexp("x", "tex."), eq("x", "test"));
		assertEquals(alwaysFalse(), predicate.simplify());

		predicate = and(regexp("x", ""), eq("x", "test"));
		assertEquals(alwaysFalse(), predicate.simplify());
	}

	@Test
	public void testPredicateRegexpAndPredicateNotEq() {
		AggregationPredicate predicate;
		predicate = and(regexp("x", "tes."), notEq("x", "test"));
		assertEquals(predicate, predicate.simplify());

		predicate = and(regexp("x", ".*"), notEq("x", "test"));
		assertEquals(predicate, predicate.simplify());

		predicate = and(regexp("x", "tex."), notEq("x", "test"));
		assertEquals(regexp("x", "tex."), predicate.simplify());

		predicate = and(regexp("x", ""), notEq("x", "test"));
		assertEquals(regexp("x", ""), predicate.simplify());
	}

	@Test
	public void testPredicateNot() {
		assertEquals(alwaysFalse(), not(not(alwaysFalse())).simplify());

		assertEquals(eq("x", 100), not(notEq("x", 100)).simplify());
		assertEquals(notEq("x", 100), not(eq("x", 100)).simplify());

		assertEquals(gt("x", 100), not(le("x", 100)).simplify());
		assertEquals(ge("x", 100), not(lt("x", 100)).simplify());
		assertEquals(lt("x", 100), not(ge("x", 100)).simplify());
		assertEquals(le("x", 100), not(gt("x", 100)).simplify());
	}

	@Test
	public void testMatches() {
		Matcher matcher;

		matcher = new Matcher(notEq("campaign", 0));
		testMatches(matcher, notEq("campaign", 0), notEq("other", 0));
		assertFalse(matcher.match(notEq("other", 0)));

		matcher = new Matcher(has("campaign"));
		testMatches(matcher, has("campaign"), has("other"));
		assertFalse(matcher.match(has("other")));

		matcher = new Matcher(alwaysTrue());
		testMatches(matcher, notEq("campaign", 0), notEq("other", 0));
		assertTrue(matcher.match(notEq("other", 0)));

		testMatches(matcher, has("campaign"), has("other"));
		assertTrue(matcher.match(has("other")));
	}

	private void testMatches(Matcher matcher, AggregationPredicate belongPredicate, AggregationPredicate belongOtherPredicate) {

		assertTrue(matcher.match(belongPredicate));
		assertTrue(matcher.match(and(
				in("campaign", 1, 3, 30)
		)));
		assertTrue(matcher.match(and(
				belongPredicate,
				in("campaign", 1, 3, 30)
		)));
		assertTrue(matcher.match(and(
				belongPredicate,
				in("campaign", 1, 3, 30)
		)));
		assertTrue(matcher.match(and(
				belongOtherPredicate,
				in("campaign", 1, 3, 30)
		)));
		assertTrue(matcher.match(and(
				belongPredicate,
				belongOtherPredicate,
				in("campaign", 1, 3, 30)
		)));
		assertTrue(matcher.match(and(
				in("other", 1, 3, 30),
				in("campaign", 1, 3, 30)
		)));
		assertTrue(matcher.match(and(
				belongPredicate,
				in("other", 1, 3, 30),
				in("campaign", 1, 3, 30)
		)));
	}

	@SuppressWarnings("unchecked")
	private boolean matches(Record record, String field, String pattern) {
		AggregationPredicate aggregationPredicate = AggregationPredicates.regexp(field, pattern);
		return ClassBuilder.create(Predicate.class)
				.withMethod("test", boolean.class, singletonList(Object.class),
						aggregationPredicate.createPredicate(cast(arg(0), Record.class), Record.FIELD_TYPES))
				.defineClassAndCreateInstance(CLASS_LOADER)
				.test(record);
	}

	public static final class Record {
		@SuppressWarnings("rawtypes")
		private static final Map<String, FieldType> FIELD_TYPES = new HashMap<>();

		static {
			FIELD_TYPES.put("booleanValue", ofBoolean());
			FIELD_TYPES.put("byteValue", ofByte());
			FIELD_TYPES.put("shortValue", ofShort());
			FIELD_TYPES.put("charValue", ofChar());
			FIELD_TYPES.put("intValue", ofInt());
			FIELD_TYPES.put("longValue", ofLong());
			FIELD_TYPES.put("floatValue", ofFloat());
			FIELD_TYPES.put("doubleValue", ofDouble());
			FIELD_TYPES.put("stringValue", ofString());
		}

		public boolean booleanValue;
		public byte byteValue;
		public short shortValue;
		public char charValue;
		public int intValue;
		public long longValue;
		public float floatValue;
		public double doubleValue;
		public String stringValue;
	}

	private static final class Matcher {
		private final AggregationPredicate matchPredicate;

		private Matcher(AggregationPredicate predicate) {
			this.matchPredicate = predicate.simplify();
		}

		private boolean match(AggregationPredicate predicate) {
			AggregationPredicate simplified = predicate.simplify();
			return simplified.equals(and(matchPredicate, simplified).simplify());
		}
	}
}
