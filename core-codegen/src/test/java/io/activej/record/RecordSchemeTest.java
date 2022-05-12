package io.activej.record;

import io.activej.codegen.ClassBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;

import static io.activej.codegen.TestUtils.assertStaticConstantsCleared;
import static org.junit.Assert.*;

public class RecordSchemeTest {

	@Before
	public void setUp() {
		ClassBuilder.clearStaticConstants();
	}

	@Test
	public void test() {
		RecordScheme scheme = RecordScheme.create()
				.withField("id", int.class)
				.withField("name", String.class)
				.build();

		Record record1 = scheme.record();
		RecordScheme scheme1 = record1.getScheme();
		assertSame(scheme, scheme1);

		record1.set("id", 10);
		assertEquals(10, record1.getInt("id"));

		record1.set("name", "abc");
		assertEquals("abc", record1.get("name"));

		Record record2 = scheme.record();
		assertNotSame(record1, record2);
		assertNotEquals(record1, record2);
		assertNotEquals(record1.hashCode(), record2.hashCode());

		record2.set("id", 10);
		record2.set("name", "abc");
		assertEquals(record1, record2);
		assertEquals(record1.hashCode(), record2.hashCode());

		RecordScheme scheme2 = RecordScheme.create(scheme1.getClassLoader())
				.withField("id", int.class)
				.withField("name", String.class)
				.build();

		assertSame(scheme.getRecordClass(), scheme2.getRecordClass());
		assertStaticConstantsCleared();
	}

	@Test
	public void test2() {
		RecordScheme scheme = RecordScheme.create()
				.withField("boolean", boolean.class)
				.withField("char", char.class)
				.withField("byte", byte.class)
				.withField("short", short.class)
				.withField("int", int.class)
				.withField("long", long.class)
				.withField("float", float.class)
				.withField("double", double.class)
				.withField("Boolean", Boolean.class)
				.withField("Character", Character.class)
				.withField("Byte", Byte.class)
				.withField("Short", Short.class)
				.withField("Integer", Integer.class)
				.withField("Long", Long.class)
				.withField("Float", Float.class)
				.withField("Double", Double.class)
				.build();

		Record record = scheme.record();

		record.set("boolean", true);
		assertEquals(true, record.get("boolean"));

		record.set("Boolean", true);
		assertEquals(true, record.get("Boolean"));

		record.set("int", 1);
		assertEquals(1, record.getInt("int"));
		assertEquals(1, scheme.getter("int").getInt(record));
		assertEquals(1, scheme.getter("int").getLong(record));
		assertEquals(1, scheme.getter("int").getFloat(record), 1E-6);
		assertEquals(1, scheme.getter("int").getDouble(record), 1E-6);
		scheme.setter("int").setInt(record, 2);
		assertEquals(2, record.getInt("int"));

		record.set("Integer", 1);
		assertEquals(1, record.getInt("Integer"));
		assertEquals(1, scheme.getter("Integer").getInt(record));
		assertEquals(1, scheme.getter("Integer").getLong(record));
		assertEquals(1, scheme.getter("Integer").getFloat(record), 1E-6);
		assertEquals(1, scheme.getter("Integer").getDouble(record), 1E-6);
		scheme.setter("Integer").setInt(record, 2);
		assertEquals(2, record.getInt("Integer"));

		assertSame(scheme, scheme.setter("int").getScheme());
		assertEquals("int", scheme.setter("int").getField());
		assertEquals(int.class, scheme.setter("int").getType());

		assertSame(scheme, scheme.getter("int").getScheme());
		assertEquals("int", scheme.getter("int").getField());
		assertEquals(int.class, scheme.getter("int").getType());
		assertStaticConstantsCleared();
	}

	@Test
	public void testHashCodeEqualsDefault() {
		RecordScheme scheme = RecordScheme.create()
				.withField("id", int.class)
				.withField("code", long.class)
				.withField("name", String.class)
				.withField("complex.name", String.class)
				.build();

		Record record1 = scheme.record();
		Record record2 = scheme.record();

		record1.set("id", 10);
		record1.set("code", 100L);
		record1.set("name", "abc");
		record1.set("complex.name", "def");

		record2.set("id", 10);
		record2.set("code", 100L);
		record2.set("name", "abc");
		record2.set("complex.name", "def");

		assertEquals(record1, record2);
		assertEquals(record1.hashCode(), record2.hashCode());

		record1.set("complex.name", "def2");

		assertNotEquals(record1, record2);
		assertNotEquals(record1.hashCode(), record2.hashCode());

		assertStaticConstantsCleared();
	}

	@Test
	public void testHashCodeEquals() {
		RecordScheme scheme = RecordScheme.create()
				.withField("id", int.class)
				.withField("code", long.class)
				.withField("name", String.class)
				.withField("complex.name", String.class)
				.withHashCodeEqualsFields("id", "code", "complex.name")
				.build();

		Record record1 = scheme.record();
		Record record2 = scheme.record();

		record1.set("id", 10);
		record1.set("code", 100L);
		record1.set("name", "abc");
		record1.set("complex.name", "def");

		record2.set("id", 10);
		record2.set("code", 100L);
		record2.set("name", "abc");
		record2.set("complex.name", "def");

		assertEquals(record1, record2);
		assertEquals(record1.hashCode(), record2.hashCode());

		record1.set("name", "abc2");
		assertEquals(record1, record2);
		assertEquals(record1.hashCode(), record2.hashCode());

		record1.set("complex.name", "def2");
		assertNotEquals(record1, record2);
		assertNotEquals(record1.hashCode(), record2.hashCode());

		record1.set("id", 10);
		record1.set("code", 200L);
		assertNotEquals(record1, record2);
		assertNotEquals(record1.hashCode(), record2.hashCode());

		assertStaticConstantsCleared();
	}

	@Test
	public void testComparator() {
		RecordScheme scheme = RecordScheme.create()
				.withField("id", int.class)
				.withField("code", long.class)
				.withField("name", String.class)
				.withField("complex.name", String.class)
				.withComparator("id", "code", "complex.name")
				.build();

		Comparator<Record> comparator = scheme.recordComparator();

		Record record1 = scheme.record();
		Record record2 = scheme.record();

		record1.set("id", 10);
		record1.set("code", 100L);
		record1.set("name", "abc");
		record1.set("complex.name", "def");

		record2.set("id", 10);
		record2.set("code", 100L);
		record2.set("name", "abc");
		record2.set("complex.name", "def");

		assertEquals(0, comparator.compare(record1, record2));

		record1.set("name", "abc2");
		assertEquals(0, comparator.compare(record1, record2));

		record1.set("name", "abc");
		record1.set("id", 5);
		assertEquals(-1, comparator.compare(record1, record2));

		record1.set("code", 200L);
		assertEquals(-1, comparator.compare(record1, record2));

		record1.set("id", 10);
		assertEquals(1, comparator.compare(record1, record2));

		assertStaticConstantsCleared();
	}
}
