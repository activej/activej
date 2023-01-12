package io.activej.cube.http;

import io.activej.aggregation.PredicateDef;
import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;

import static io.activej.aggregation.AggregationPredicates.*;
import static io.activej.cube.Utils.fromJson;
import static io.activej.cube.Utils.toJson;

public class JsonCodec_PredicateDef_Test {
	private static final JsonCodec_AggregationPredicate CODEC = JsonCodec_AggregationPredicate.create(
			Map.of(
					"campaign", int.class,
					"site", String.class,
					"hourOfDay", int.class),
			Map.of(
					"eventCount", int.class,
					"ctr", double.class)
	);

	@Test
	public void testAlwaysTrue() {
		doTest(alwaysTrue());
	}

	@Test
	public void testAlwaysFalse() {
		doTest(alwaysFalse());
	}

	@Test
	public void testNot() {
		doTest(not(alwaysTrue()));
		doTest(not(alwaysFalse()));
		doTest(not(eq("site", "test")));
	}

	@Test
	public void testAnd() {
		doTest(and(alwaysFalse(), alwaysTrue()));
		doTest(and(eq("site", "test"), ge("campaign", 123)));
	}

	@Test
	public void testOr() {
		doTest(or(alwaysFalse(), alwaysTrue()));
		doTest(or(eq("site", "test"), ge("campaign", 123)));
	}

	@Test
	public void testEq() {
		doTest(eq("site", "test"));
		doTest(eq("site", null));
		doTest(eq("campaign", 1234));
	}

	@Test
	public void testNotEq() {
		doTest(notEq("site", "test"));
		doTest(notEq("campaign", 1234));
		doTest(notEq("campaign", null));
	}

	@Test
	public void testGe() {
		doTest(ge("campaign", 1234));
		doTest(ge("campaign", null));
	}

	@Test
	public void testLe() {
		doTest(le("ctr", 12.34));
	}

	@Test
	public void testGt() {
		doTest(gt("eventCount", 1234));
	}

	@Test
	public void testLt() {
		doTest(lt("campaign", 1234));
	}

	@Test
	public void testIn() {
		doTest(in("campaign", 12, 23, 43, 54, 65));
		doTest(in("campaign", 12));
		doTest(in("hourOfDay", 0, 23));
		doTest(in("hourOfDay"));
	}

	@Test
	public void testRegexp() {
		doTest(regexp("regexp", "pattern"));
	}

	@Test
	public void testBetween() {
		doTest(between("campaign", -12, 444));
	}

	@Test
	public void testHas() {
		doTest(has("test"));
		doTest(has("campaign"));
	}

	private static void doTest(PredicateDef predicate) {
		String json = toJson(CODEC, predicate);
		PredicateDef decoded;
		try {
			decoded = fromJson(CODEC, json);
		} catch (MalformedDataException e) {
			throw new AssertionError(e);
		}
		if (!Objects.equals(predicate, decoded)) {
			throw new AssertionError();
		}
	}
}
