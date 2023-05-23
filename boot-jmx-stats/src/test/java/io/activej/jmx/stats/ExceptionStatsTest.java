package io.activej.jmx.stats;

import org.junit.Before;
import org.junit.Test;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import static org.junit.Assert.assertEquals;

public class ExceptionStatsTest {
	String[] exceptionDetailsItemNames;
	OpenType<?>[] exceptionDetailsItemTypes;

	@Before
	public void before() throws OpenDataException {
		exceptionDetailsItemNames = new String[]{
			"lastException",
			"lastExceptionCausedObject",
			"lastExceptionStackTrace",
			"lastExceptionTimestamp",
			"totalExceptions"
		};

		exceptionDetailsItemTypes = new OpenType<?>[]{
			SimpleType.STRING,
			SimpleType.STRING,
			new ArrayType<>(1, SimpleType.STRING),
			SimpleType.LONG,
			SimpleType.INTEGER
		};
	}

	@Test
	public void itShouldProperlyCollectAttributes() {
		ExceptionStats stats = ExceptionStats.create();
		Exception e = new RuntimeException("msg");
		Object causedObject = "cause";
		stats.recordException(e, causedObject);

		assertEquals("msg", stats.getLastMessage());
		assertEquals(1, stats.getTotal());
	}

	@Test
	public void itShouldProperlyAggregateAttributes() {
		// init and record
		ExceptionStats stats_1 = ExceptionStats.create();
		Exception exception_1 = new RuntimeException("msg-1");
		Object causedObject_1 = "cause-1";
		stats_1.recordException(exception_1, causedObject_1);

		ExceptionStats stats_2 = ExceptionStats.create();
		Exception exception_2 = new RuntimeException("msg-2");
		Object causedObject_2 = "cause-2";
		stats_2.recordException(exception_2, causedObject_2);

		ExceptionStats stats_3 = ExceptionStats.create();
		Exception exception_3 = new RuntimeException("msg-3");
		Object causedObject_3 = "cause-3";
		stats_3.recordException(exception_3, causedObject_3);

		// aggregate
		ExceptionStats accumulator = ExceptionStats.create();
		accumulator.add(stats_1);
		accumulator.add(stats_2);
		accumulator.add(stats_3);

		assertEquals(3, accumulator.getTotal());
	}
}
