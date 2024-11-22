package io.activej.cube.http;

import io.activej.codegen.ClassGenerator;
import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.measure.Measures;
import io.activej.cube.measure.ComputedMeasure;
import io.activej.cube.measure.ComputedMeasures;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.collection.CollectorUtils.toLinkedHashMap;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofDouble;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class ComputedMeasuresTest {
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();
	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	private static final class M extends Measures {}

	private static final class CM extends ComputedMeasures {}

	public interface TestQueryResultPlaceholder {
		void computeMeasures();

		void init();

		Object getResult();
	}

	private static final Map<String, Measure> MEASURES =
		Stream.of("a", "b", "c", "d")
			.collect(toLinkedHashMap(k -> M.sum(ofDouble())));

	@Test
	public void test() {
		ComputedMeasure d = CM.div(
			CM.mul(
				CM.div(
					CM.measure("a"),
					CM.measure("b")
				),
				CM.value(100)
			),
			CM.measure("c")
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassGenerator.builder(TestQueryResultPlaceholder.class)
			.withField("a", long.class)
			.withField("b", long.class)
			.withField("c", double.class)
			.withField("d", double.class)
			.withMethod("computeMeasures", set(property(self(), "d"), d.getExpression(self(), MEASURES)))
			.withMethod("init", sequence(
				set(property(self(), "a"), value(1)),
				set(property(self(), "b"), value(100)),
				set(property(self(), "c"), value(5))))
			.withMethod("getResult", property(self(), "d"))
			.build()
			.generateClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() {
		ComputedMeasure d = CM.div(
			CM.mul(
				CM.div(
					CM.measure("a"),
					CM.measure("b")
				),
				CM.value(100)
			),
			CM.measure("c")
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassGenerator.builder(TestQueryResultPlaceholder.class)
			.withField("a", long.class)
			.withField("b", long.class)
			.withField("c", double.class)
			.withField("d", double.class)
			.withMethod("computeMeasures", set(property(self(), "d"), d.getExpression(self(), MEASURES)))
			.withMethod("init", sequence(
				set(property(self(), "a"), value(1)),
				set(property(self(), "b"), value(0)),
				set(property(self(), "c"), value(0))))
			.withMethod("getResult", property(self(), "d"))
			.build()
			.generateClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() {
		ComputedMeasure c = CM.sqrt(
			CM.add(
				CM.measure("a"),
				CM.measure("b")
			)
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassGenerator.builder(TestQueryResultPlaceholder.class)
			.withField("a", double.class)
			.withField("b", double.class)
			.withField("c", double.class)
			.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
			.withMethod("init", sequence(
				set(property(self(), "a"), value(2.0)),
				set(property(self(), "b"), value(7.0))))
			.withMethod("getResult", property(self(), "c"))
			.build()
			.generateClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() {
		ComputedMeasure c = CM.sqrt(
			CM.sub(
				CM.measure("a"),
				CM.measure("b")
			)
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassGenerator.builder(TestQueryResultPlaceholder.class)
			.withField("a", double.class)
			.withField("b", double.class)
			.withField("c", double.class)
			.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
			.withMethod("init", sequence(
				set(property(self(), "a"), value(0.0)),
				set(property(self(), "b"), value(1E-10))))
			.withMethod("getResult", property(self(), "c"))
			.build()
			.generateClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
