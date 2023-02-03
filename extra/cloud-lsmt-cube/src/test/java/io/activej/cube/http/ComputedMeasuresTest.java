package io.activej.cube.http;

import io.activej.aggregation.measure.Measure;
import io.activej.aggregation.measure.Measures;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.measure.ComputedMeasure;
import io.activej.cube.measure.ComputedMeasures;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Stream;

import static io.activej.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Utils.keysToMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class ComputedMeasuresTest {
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();
	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	public interface TestQueryResultPlaceholder {
		void computeMeasures();

		void init();

		Object getResult();
	}

	private static final Map<String, Measure> MEASURES =
			keysToMap(Stream.of("a", "b", "c", "d"), k -> Measures.sum(ofDouble()));

	@Test
	public void test() {
		ComputedMeasure d = ComputedMeasures.div(
				ComputedMeasures.mul(
						ComputedMeasures.div(
								ComputedMeasures.measure("a"),
								ComputedMeasures.measure("b")
						),
						ComputedMeasures.value(100)
				),
				ComputedMeasures.measure("c")
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.builder(TestQueryResultPlaceholder.class)
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
				.defineClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() {
		ComputedMeasure d = ComputedMeasures.div(
				ComputedMeasures.mul(
						ComputedMeasures.div(
								ComputedMeasures.measure("a"),
								ComputedMeasures.measure("b")
						),
						ComputedMeasures.value(100)
				),
				ComputedMeasures.measure("c")
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.builder(TestQueryResultPlaceholder.class)
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
				.defineClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() {
		ComputedMeasure c = ComputedMeasures.sqrt(
				ComputedMeasures.add(
						ComputedMeasures.measure("a"),
						ComputedMeasures.measure("b")
				)
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.builder(TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(2.0)),
						set(property(self(), "b"), value(7.0))))
				.withMethod("getResult", property(self(), "c"))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() {
		ComputedMeasure c = ComputedMeasures.sqrt(
				ComputedMeasures.sub(
						ComputedMeasures.measure("a"),
						ComputedMeasures.measure("b")
				)
		);

		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.builder(TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(0.0)),
						set(property(self(), "b"), value(1E-10))))
				.withMethod("getResult", property(self(), "c"))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
