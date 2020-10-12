package io.activej.record;

import io.activej.codegen.expression.Expression;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.activej.codegen.expression.Expressions.add;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class RecordProjectionTest {

	@Test
	public void test1() {
		RecordScheme schemeFrom = RecordScheme.create()
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
	}

	@Test
	public void test2() {
		RecordScheme schemeFrom = RecordScheme.create()
				.withField("int", int.class)
				.withField("Integer", Integer.class)
				.build();

		RecordScheme schemeTo = RecordScheme.create(schemeFrom.getClassLoader())
				.withField("x", int.class)
				.build();

		Map<String, Function<Expression, Expression>> mapping = new HashMap<>();
		mapping.put("x", recordFrom -> add(schemeFrom.property(recordFrom, "int"), schemeFrom.property(recordFrom, "Integer")));

		RecordProjection projection = RecordProjection.projection(schemeFrom, schemeTo, mapping);

		Record record1 = schemeFrom.recordOfArray(10, 100);

		Record record2 = projection.apply(record1);

		assertEquals(110, record2.getInt("x"));

		Function<Record, Record> fn = projection;
		Record record3 = fn.apply(record1);
		assertEquals(110, record3.getInt("x"));
		record3.set("x", 0);
		assertEquals(0, record3.getInt("x"));
		((BiConsumer<Record, Record>) projection).accept(record1, record3);
		assertEquals(110, record3.getInt("x"));
	}
}
