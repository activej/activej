package io.activej.record;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.activej.codegen.TestUtils.assertStaticConstantsCleared;
import static io.activej.codegen.expression.Expression.add;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class RecordProjectionTest {
	@Before
	public void setUp() {
		ClassBuilder.clearStaticConstants();
	}

	@Test
	public void test1() {
		RecordScheme schemeFrom = RecordScheme.builder()
				.withField("int", int.class)
				.withField("Integer", Integer.class)
				.build();

		RecordProjection projection = RecordProjection.projection(schemeFrom, "int", "Integer");

		Record record1 = schemeFrom.recordOfArray(10, 100);

		Record record2 = projection.apply(record1);

		assertEquals(10, record2.getInt("int"));
		assertEquals((Integer) 10, record2.get("int"));
		assertEquals(100, record2.getInt("Integer"));
		assertEquals((Integer) 100, record2.get("Integer"));

		RecordProjection projection2 = RecordProjection.projection(schemeFrom.getClassLoader(), schemeFrom, "int", "Integer");
		RecordProjection projection3 = RecordProjection.projection(schemeFrom.getClassLoader(), schemeFrom, "int", "Integer");
		assertSame(projection2.getClass(), projection3.getClass());
		assertStaticConstantsCleared();
	}

	@Test
	public void test2() {
		RecordScheme schemeFrom = RecordScheme.builder()
				.withField("int", int.class)
				.withField("Integer", Integer.class)
				.build();

		RecordScheme schemeTo = RecordScheme.builder(schemeFrom.getClassLoader())
				.withField("x", int.class)
				.build();

		Map<String, UnaryOperator<Expression>> mapping = new HashMap<>();
		mapping.put("x", recordFrom -> add(schemeFrom.property(recordFrom, "int"), schemeFrom.property(recordFrom, "Integer")));

		RecordProjection projection = RecordProjection.projection(schemeFrom, schemeTo, mapping);

		Record record1 = schemeFrom.recordOfArray(10, 100);

		Record record2 = projection.apply(record1);

		assertEquals(110, record2.getInt("x"));

		Record record3 = projection.apply(record1);
		assertEquals(110, record3.getInt("x"));
		record3.set("x", 0);
		assertEquals(0, record3.getInt("x"));
		projection.accept(record1, record3);
		assertEquals(110, record3.getInt("x"));
		assertStaticConstantsCleared();
	}
}
