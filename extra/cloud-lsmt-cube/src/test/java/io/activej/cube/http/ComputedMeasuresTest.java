package io.activej.cube.http;

import io.activej.aggregation.measure.Measure;
import io.activej.aggregation.measure.Measures;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.ComputedMeasure;
import io.activej.cube.ComputedMeasures;
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
			keysToMap(Stream.of("a", "b", "c", "d"), k -> M.sum(ofDouble()));

	@Test
	public void test() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = CM.div(CM.mul(CM.div(CM.measure("a"), CM.measure("b")), CM.value(100)), CM.measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
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
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.2, resultPlaceholder.getResult());
		assertEquals(Stream.of("a", "b", "c").collect(toSet()), d.getMeasureDependencies());
	}

	@Test
	public void testNullDivision() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure d = CM.div(CM.mul(CM.div(CM.measure("a"), CM.measure("b")), CM.value(100)), CM.measure("c"));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
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
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrt() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = CM.sqrt(CM.add(CM.measure("a"), CM.measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(2.0)),
						set(property(self(), "b"), value(7.0))))
				.withMethod("getResult", property(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(3.0, resultPlaceholder.getResult());
	}

	@Test
	public void testSqrtOfNegativeArgument() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		ComputedMeasure c = CM.sqrt(CM.sub(CM.measure("a"), CM.measure("b")));
		TestQueryResultPlaceholder resultPlaceholder = ClassBuilder.create(classLoader, TestQueryResultPlaceholder.class)
				.withField("a", double.class)
				.withField("b", double.class)
				.withField("c", double.class)
				.withMethod("computeMeasures", set(property(self(), "c"), c.getExpression(self(), MEASURES)))
				.withMethod("init", sequence(
						set(property(self(), "a"), value(0.0)),
						set(property(self(), "b"), value(1E-10))))
				.withMethod("getResult", property(self(), "c"))
				.buildClassAndCreateNewInstance();
		resultPlaceholder.init();
		resultPlaceholder.computeMeasures();

		assertEquals(0.0, resultPlaceholder.getResult());
	}
}
