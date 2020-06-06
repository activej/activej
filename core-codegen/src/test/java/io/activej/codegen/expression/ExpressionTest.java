package io.activej.codegen.expression;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.operation.ArithmeticOperation;
import io.activej.codegen.operation.CompareOperation;
import io.activej.common.api.Initializer;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static io.activej.codegen.expression.ExpressionComparator.leftProperty;
import static io.activej.codegen.expression.ExpressionComparator.rightProperty;
import static io.activej.codegen.expression.Expressions.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class ExpressionTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public static class TestPojo {
		public int property1;
		public int property2;

		public TestPojo(int property1, int property2) {
			this.property1 = property1;
			this.property2 = property2;
		}

		public TestPojo(int property1) {
			this.property1 = property1;
		}

		public void setproperty1(int property1) {
			this.property1 = property1;
		}

		public int getproperty1() {
			return property1;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo testPojo = (TestPojo) o;

			return (property1 == testPojo.property1) && (property2 == testPojo.property2);
		}

		@Override
		public int hashCode() {
			int result = property1;
			result = 31 * result + property2;
			return result;
		}

		@Override
		public String toString() {
			return "TestPojo{property1=" + property1 + ", property2=" + property2 + '}';
		}
	}

	public static class TestPojo2 {
		public String property1;
		public int property2;
		public long property3;
		public float property4;
		public int property5;
		public double property6;
		public String property7;

		public TestPojo2(String property1, int property2, long property3, float property4, int property5, double property6, String property7) {
			this.property1 = property1;
			this.property2 = property2;
			this.property3 = property3;
			this.property4 = property4;
			this.property5 = property5;
			this.property6 = property6;
			this.property7 = property7;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo2 testPojo2 = (TestPojo2) o;

			if (property2 != testPojo2.property2) return false;
			if (property3 != testPojo2.property3) return false;
			if (Float.compare(testPojo2.property4, property4) != 0) return false;
			if (property5 != testPojo2.property5) return false;
			if (Double.compare(testPojo2.property6, property6) != 0) return false;
			if (!Objects.equals(property1, testPojo2.property1)) return false;
			return Objects.equals(property7, testPojo2.property7);

		}

		@Override
		public int hashCode() {
			int result;
			long temp;
			result = property1 != null ? property1.hashCode() : 0;
			result = 31 * result + property2;
			result = 31 * result + (int) (property3 ^ (property3 >>> 32));
			result = 31 * result + (property4 != +0.0f ? Float.floatToIntBits(property4) : 0);
			result = 31 * result + property5;
			temp = Double.doubleToLongBits(property6);
			result = 31 * result + (int) (temp ^ (temp >>> 32));
			result = 31 * result + (property7 != null ? property7.hashCode() : 0);
			return result;
		}
	}

	public interface Test2 {
		int hash(TestPojo2 pojo);
	}

	public interface Test extends Comparator<TestPojo>, Comparable<Test> {
		Integer test(Integer argument);

		int hash(TestPojo pojo);

		int property1(TestPojo pojo);

		TestPojo setter(TestPojo pojo);

		TestPojo ctor();

		void setXY(int valueX, byte valueY);

		Integer getX();

		short getY();

		boolean allEqual(int var, int var1, int var2);

		boolean anyEqual(int var, int var1, int var2);

		void setPojoproperty1(TestPojo testPojo, int value);

		int getPojoproperty1(TestPojo testPojo);

		@Override
		int compare(TestPojo o1, TestPojo o2);

		@Override
		boolean equals(Object obj);

		@Override
		int compareTo(@NotNull Test o);

		@Override
		String toString();

		void loop();
	}

	@org.junit.Test
	public void test() throws IllegalAccessException, InstantiationException {
		Class<Test> testClass = ClassBuilder.create(DefiningClassLoader.create(), Test.class)
				.withField("x", int.class)
				.withField("y", Long.class)
				.withMethod("compare", int.class, asList(TestPojo.class, TestPojo.class),
						compare(TestPojo.class, "property1", "property2"))
				.withMethod("int compareTo(io.activej.codegen.expression.ExpressionTest$Test)",
						compareToImpl("x"))
				.withMethod("equals",
						equalsImpl("x"))
				.withMethod("setXY", sequence(
						set(property(self(), "x"), arg(0)),
						set(property(self(), "y"), arg(1))))
				.withMethod("test",
						add(arg(0), value(1L)))
				.withMethod("hash",
						hash(property(arg(0), "property1"), property(arg(0), "property2")))
				.withMethod("property1",
						property(arg(0), "property1"))
				.withMethod("setter", sequence(
						set(property(arg(0), "property1"), value(10)),
						set(property(arg(0), "property2"), value(20)),
						arg(0)))
				.withMethod("ctor", let(
						constructor(TestPojo.class, value(1)),
						instance -> sequence(
								set(property(instance, "property2"), value(2)),
								instance)))
				.withMethod("getX",
						property(self(), "x"))
				.withMethod("getY",
						property(self(), "y"))
				.withMethod("allEqual",
						and(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("anyEqual",
						or(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("setPojoproperty1",
						call(arg(0), "setproperty1", arg(1)))
				.withMethod("getPojoproperty1",
						call(arg(0), "getproperty1"))
				.withMethod("toString",
						ExpressionToString.create()
								.withQuotes("{", "}", ", ")
								.with(property(self(), "x"))
								.with("labelY: ", property(self(), "y")))
				.build();
		Test test = testClass.newInstance();

		assertEquals(11, (int) test.test(10));
		assertEquals(33, test.hash(new TestPojo(1, 2)));
		assertEquals(1, test.property1(new TestPojo(1, 2)));
		assertEquals(new TestPojo(10, 20), test.setter(new TestPojo(1, 2)));
		assertEquals(new TestPojo(1, 2), test.ctor());
		test.setXY(1, (byte) 10);
		assertEquals(1, (int) test.getX());
		assertEquals(10, test.getY());
		assertEquals(0, test.compare(new TestPojo(1, 10), new TestPojo(1, 10)));
		assertTrue(test.compare(new TestPojo(2, 10), new TestPojo(1, 10)) > 0);
		assertTrue(test.compare(new TestPojo(0, 10), new TestPojo(1, 10)) < 0);
		assertTrue(test.compare(new TestPojo(1, 0), new TestPojo(1, 10)) < 0);

		Test test1 = testClass.newInstance();
		Test test2 = testClass.newInstance();

		test1.setXY(1, (byte) 10);
		test2.setXY(1, (byte) 10);
		assertEquals(0, test1.compareTo(test2));
		assertEquals(test1, test2);
		test2.setXY(2, (byte) 10);
		assertTrue(test1.compareTo(test2) < 0);
		assertNotEquals(test1, test2);
		test2.setXY(0, (byte) 10);
		assertTrue(test1.compareTo(test2) > 0);
		assertNotEquals(test1, test2);

		assertTrue(test1.allEqual(1, 1, 1));
		assertFalse(test1.allEqual(1, 2, 1));
		assertFalse(test1.allEqual(1, 1, 2));
		assertFalse(test1.anyEqual(1, 2, 3));
		assertTrue(test1.anyEqual(1, 2, 1));
		assertTrue(test1.anyEqual(1, 1, 2));

		TestPojo testPojo = new TestPojo(1, 10);
		assertEquals(1, test1.getPojoproperty1(testPojo));
		test1.setPojoproperty1(testPojo, 2);
		assertEquals(2, test1.getPojoproperty1(testPojo));

		assertEquals("{1, labelY: 10}", test1.toString());
	}

	@org.junit.Test
	public void test2() throws IllegalAccessException, InstantiationException {
		Class<Test2> testClass = ClassBuilder.create(DefiningClassLoader.create(), Test2.class)
				.withMethod("hash",
						hash(
								property(arg(0), "property1"),
								property(arg(0), "property2"),
								property(arg(0), "property3"),
								property(arg(0), "property4"),
								property(arg(0), "property5"),
								property(arg(0), "property6"),
								property(arg(0), "property7")))
				.build();

		Test2 test = testClass.newInstance();
		TestPojo2 testPojo2 = new TestPojo2("randomString", 42, 666666, 43258.42342f, 54359878, 43252353278423.423468, "fhsduighrwqruqsd");

		assertEquals(testPojo2.hashCode(), test.hash(testPojo2));
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparator() {
		Comparator<TestPojo> comparator = ClassBuilder.create(DefiningClassLoader.create(), Comparator.class)
				.withMethod("compare",
						compare(TestPojo.class, "property1", "property2"))
				.buildClassAndCreateNewInstance();
		assertEquals(0, comparator.compare(new TestPojo(1, 10), new TestPojo(1, 10)));
	}

	public interface TestNeg {
		boolean negBoolean();

		int negByte();

		int negShort();

		int negChar();

		int negInt();

		long negLong();

		float negFloat();

		double negDouble();
	}

	@org.junit.Test
	public void testNeg() {
		byte b = Byte.MAX_VALUE;
		short s = Short.MAX_VALUE;
		char c = Character.MAX_VALUE;
		int i = Integer.MAX_VALUE;
		long l = Long.MAX_VALUE;
		float f = Float.MAX_VALUE;
		double d = Double.MAX_VALUE;

		TestNeg testClass = ClassBuilder.create(DefiningClassLoader.create(), TestNeg.class)
				.withMethod("negBoolean", neg(value(true)))
				.withMethod("negShort", neg(value(s)))
				.withMethod("negByte", neg(value(b)))
				.withMethod("negChar", neg(value(c)))
				.withMethod("negInt", neg(value(i)))
				.withMethod("negLong", neg(value(l)))
				.withMethod("negFloat", neg(value(f)))
				.withMethod("negDouble", neg(value(d)))
				.buildClassAndCreateNewInstance();

		assertFalse(testClass.negBoolean());
		assertEquals(testClass.negShort(), -s);
		assertEquals(testClass.negByte(), -b);
		assertEquals(testClass.negChar(), -c);
		assertEquals(testClass.negInt(), -i);
		assertEquals(testClass.negLong(), -l);
		assertEquals(testClass.negFloat(), -f, 0.0);
		assertEquals(testClass.negDouble(), -d, 0.0);
	}

	public interface TestOperation {
		int remB();

		int remS();

		int remC();

		int remI();

		long remL();

		float remF();

		double remD();
	}

	@org.junit.Test
	public void testOperation() {
		byte b = Byte.MAX_VALUE;
		short s = Short.MAX_VALUE;
		char c = Character.MAX_VALUE;
		int i = Integer.MAX_VALUE;
		long l = Long.MAX_VALUE;
		float f = Float.MAX_VALUE;
		double d = Double.MAX_VALUE;

		TestOperation testClass = ClassBuilder.create(DefiningClassLoader.create(), TestOperation.class)
				.withMethod("remB", arithmeticOp(ArithmeticOperation.REM, value(b), value(20)))
				.withMethod("remS", arithmeticOp(ArithmeticOperation.REM, value(s), value(20)))
				.withMethod("remC", arithmeticOp(ArithmeticOperation.REM, value(c), value(20)))
				.withMethod("remI", arithmeticOp(ArithmeticOperation.REM, value(i), value(20)))
				.withMethod("remL", arithmeticOp(ArithmeticOperation.REM, value(l), value(20)))
				.withMethod("remF", arithmeticOp(ArithmeticOperation.REM, value(f), value(20)))
				.withMethod("remD", arithmeticOp(ArithmeticOperation.REM, value(d), value(20)))
				.buildClassAndCreateNewInstance();

		assertEquals(testClass.remB(), b % 20);
		assertEquals(testClass.remS(), s % 20);
		assertEquals(testClass.remC(), c % 20);
		assertEquals(testClass.remI(), i % 20);
		assertEquals(testClass.remL(), l % 20);
		assertEquals(testClass.remF(), f % 20, 0.0);
		assertEquals(testClass.remD(), d % 20, 0.0);
	}

	public interface TestSH {
		int shlInt();

		long shlLong();

		int shrInt();

		long shrLong();

		int ushrInt();
	}

	@org.junit.Test
	public void testSH() {
		byte b = 8;
		int i = 2;
		long l = 4;

		TestSH testClass = ClassBuilder.create(DefiningClassLoader.create(), TestSH.class)
				.withMethod("shlInt", shl(value(b), value(i)))
				.withMethod("shlLong", shl(value(l), value(b)))
				.withMethod("shrInt", shr(value(b), value(i)))
				.withMethod("shrLong", shr(value(l), value(i)))
				.withMethod("ushrInt", ushr(value(b), value(i)))
				.buildClassAndCreateNewInstance();

		assertEquals(testClass.shlInt(), b << i);
		assertEquals(testClass.shlLong(), l << b);
		assertEquals(testClass.shrInt(), b >> i);
		assertEquals(testClass.shrLong(), l >> i);
		assertEquals(testClass.ushrInt(), b >>> i);
	}

	public interface TestBitMask {
		int andInt();

		int orInt();

		int xorInt();

		long andLong();

		long orLong();

		long xorLong();
	}

	@org.junit.Test
	public void testBitMask() {
		TestBitMask testClass = ClassBuilder.create(DefiningClassLoader.create(), TestBitMask.class)
				.withMethod("andInt", bitAnd(value(2), value(4)))
				.withMethod("orInt", bitOr(value(2), value(4)))
				.withMethod("xorInt", bitXor(value(2), value(4)))
				.withMethod("andLong", bitAnd(value(2), value(4L)))
				.withMethod("orLong", bitOr(value((byte) 2), value(4L)))
				.withMethod("xorLong", bitXor(value(2L), value(4L)))
				.buildClassAndCreateNewInstance();

		assertEquals(testClass.andInt(), 2 & 4);
		assertEquals(testClass.orInt(), 2 | 4);
		assertEquals(testClass.xorInt(), 2 ^ 4);
		assertEquals(testClass.andLong(), 2L & 4L);
		assertEquals(testClass.orLong(), 2L | 4L);
		assertEquals(testClass.xorLong(), 2L ^ 4L);
	}

	public interface TestCall {
		int callOther1(int i);

		long callOther2();

		int callStatic1(int i1, int i2);

		long callStatic2(long l);
	}

	@org.junit.Test
	public void testCall() {
		TestCall testClass = ClassBuilder.create(DefiningClassLoader.create(), TestCall.class)
				.withMethod("callOther1", call(self(), "method", arg(0)))
				.withMethod("callOther2", call(self(), "method"))
				.withMethod("method", int.class, asList(int.class), arg(0))
				.withMethod("method", long.class, asList(), value(-1L))
				.withMethod("callStatic1", int.class, asList(int.class, int.class), staticCallSelf("method", arg(0), arg(1)))
				.withMethod("callStatic2", long.class, asList(long.class), staticCallSelf("method", arg(0)))
				.withStaticMethod("method", int.class, asList(int.class, int.class), arg(1))
				.withStaticMethod("method", long.class, asList(long.class), arg(0))
				.buildClassAndCreateNewInstance();

		assertEquals(100, testClass.callOther1(100));
		assertEquals(-1, testClass.callOther2());
		assertEquals(2, testClass.callStatic1(1, 2));
		assertEquals(3L, testClass.callStatic2(3L));
	}

	public interface TestArgument {
		Object array(WriteFirstElement w, Object[] arr);

		Object write(WriteFirstElement w, Object o);
	}

	public static class WriteFirstElement {
		public Object writeFirst(Object[] i) {
			return i[0];
		}

		public Object write(Object o) {
			return o;
		}

	}

	@org.junit.Test
	public void testArgument() {
		TestArgument testClass = ClassBuilder.create(DefiningClassLoader.create(), TestArgument.class)
				.withMethod("array", call(arg(0), "writeFirst", arg(1)))
				.withMethod("write", call(arg(0), "write", arg(1)))
				.buildClassAndCreateNewInstance();

		assertEquals(1000, testClass.array(new WriteFirstElement(), new Object[]{1000, 2, 3, 4}));
		assertEquals(1000, testClass.write(new WriteFirstElement(), 1000));
	}

	public interface WriteAllListElement {
		void write(List<?> listFrom, List<?> listTo);

		void writeIter(Iterator<?> iteratorFrom, List<?> listTo);
	}

	@org.junit.Test
	public void testIterator() {
		List<Integer> listFrom = asList(1, 1, 2, 3, 5, 8);
		List<Integer> listTo1 = new ArrayList<>();
		List<Integer> listTo2 = new ArrayList<>();

		WriteAllListElement testClass = ClassBuilder.create(DefiningClassLoader.create(), WriteAllListElement.class)
				.withMethod("write",
						forEach(arg(0),
								it -> sequence(addListItem(arg(1), it), voidExp())))
				.withMethod("writeIter",
						forEach(arg(0),
								it -> sequence(addListItem(arg(1), it), voidExp())))
				.buildClassAndCreateNewInstance();

		testClass.write(listFrom, listTo1);
		testClass.writeIter(listFrom.iterator(), listTo2);

		assertEquals(listFrom.size(), listTo1.size());
		for (int i = 0; i < listFrom.size(); i++) {
			assertEquals(listFrom.get(i), listTo1.get(i));
		}

		assertEquals(listFrom.size(), listTo2.size());
		for (int i = 0; i < listFrom.size(); i++) {
			assertEquals(listFrom.get(i), listTo2.get(i));
		}
	}

	public interface WriteArrayElements {
		void write(Long[] a, List<Long> b);
	}

	@org.junit.Test
	public void testIteratorForArray() {
		Long[] intsFrom = {1L, 1L, 2L, 3L, 5L, 8L};
		List<Long> list = new ArrayList<>();

		WriteArrayElements testClass = ClassBuilder.create(DefiningClassLoader.create(), WriteArrayElements.class)
				.withMethod("write", forEach(arg(0),
						it -> sequence(addListItem(arg(1), cast(it, Object.class)), voidExp())))
				.buildClassAndCreateNewInstance();

		testClass.write(intsFrom, list);
		for (int i = 0; i < intsFrom.length; i++) {
			assertEquals(intsFrom[i], list.get(i));
		}
	}

	public interface CastPrimitive {
		Object a();
	}

	@org.junit.Test
	public void testCastPrimitive() {
		CastPrimitive testClass = ClassBuilder.create(DefiningClassLoader.create(), CastPrimitive.class)
				.withMethod("a", value(1))
				.buildClassAndCreateNewInstance();

		assertEquals(1, testClass.a());
	}

	public interface Initializable {
		void init();
	}

	public interface Getter {
		Object get(Object obj);
	}

	@org.junit.Test
	public void testGetter() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Initializable intHolder = ClassBuilder.create(classLoader, Initializable.class)
				.withField("x", int.class)
				.withMethod("init", set(property(self(), "x"), value(42)))
				.buildClassAndCreateNewInstance();

		intHolder.init();

		Getter getter = ClassBuilder.create(classLoader, Getter.class)
				.withMethod("get", property(cast(arg(0), intHolder.getClass()), "x"))
				.buildClassAndCreateNewInstance();

		assertEquals(42, getter.get(intHolder));
	}

	@org.junit.Test
	public void testBuiltInstance() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();

		Initializer<ClassBuilder<Object>> initializer = builder -> builder
				.withField("x", int.class)
				.withField("y", Long.class)
				.withMethod("compare", int.class, asList(TestPojo.class, TestPojo.class),
						compare(TestPojo.class, "property1", "property2"));

		Class<?> testClass1 = ClassBuilder.create(definingClassLoader, Object.class)
				.withClassKey("TestKey")
				.withInitializer(initializer)
				.build();

		Class<?> testClass2 = ClassBuilder.create(definingClassLoader, Object.class)
				.withClassKey("TestKey")
				.withInitializer(initializer)
				.build();

		assertEquals(testClass1, testClass2);
	}

	public interface TestCompare {
		boolean compareObjectLE(Integer i1, Integer i2);

		boolean comparePrimitiveLE(int i1, int i2);

		boolean compareObjectEQ(Integer i1, Integer i2);

		boolean compareObjectNE(Integer i1, Integer i2);

	}

	@org.junit.Test
	public void testCompare() throws IllegalAccessException, InstantiationException {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		Class<TestCompare> test1 = ClassBuilder.create(definingClassLoader, TestCompare.class)
				.withMethod("compareObjectLE", cmp(CompareOperation.LE, arg(0), arg(1)))
				.withMethod("comparePrimitiveLE", cmp(CompareOperation.LE, arg(0), arg(1)))
				.withMethod("compareObjectEQ", cmp(CompareOperation.EQ, arg(0), arg(1)))
				.withMethod("compareObjectNE", cmp(CompareOperation.NE, arg(0), arg(1)))
				.build();

		TestCompare testCompare = test1.newInstance();
		assertTrue(testCompare.compareObjectLE(5, 5));
		assertTrue(testCompare.comparePrimitiveLE(5, 6));
		assertTrue(testCompare.compareObjectEQ(5, 5));
		assertTrue(testCompare.compareObjectNE(5, -5));
	}

	public static class StringHolder {
		public String string1;
		public String string2;

		public StringHolder(String string1, String string2) {
			this.string1 = string1;
			this.string2 = string2;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			StringHolder that = (StringHolder) o;
			return Objects.equals(string1, that.string1) &&
					Objects.equals(string2, that.string2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string1, string2);
		}

		@Override
		public String toString() {
			return "StringHolder{" +
					"string1='" + string1 + '\'' +
					", string2='" + string2 + '\'' +
					'}';
		}
	}

	public static class StringHolderComparator implements Comparator<StringHolder> {
		public int compare(StringHolder var1, StringHolder var2) {
			String var1String1 = var1.string1;
			String var2String1 = var2.string1;
			int compare;
			if (var1String1 == null) {
				if (var2String1 != null) {
					return -1;
				}
			} else {
				if (var2String1 == null) {
					return 1;
				}

				compare = var1String1.compareTo(var2String1);
				if (compare != 0) {
					return compare;
				}
			}

			String var1String2 = var1.string2;
			String var2String2 = var2.string2;
			if (var1String2 == null) {
				if (var2String2 != null) {
					return -1;
				}
			} else {
				if (var2String2 == null) {
					return 1;
				}

				compare = var1String2.compareTo(var2String2);
				if (compare != 0) {
					return compare;
				}
			}

			compare = 0;
			return compare;
		}
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparatorNullable() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Comparator<StringHolder> generatedComparator = ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", ExpressionComparator.create()
						.with(leftProperty(StringHolder.class, "string1"), rightProperty(StringHolder.class, "string1"), true)
						.with(leftProperty(StringHolder.class, "string2"), rightProperty(StringHolder.class, "string2"), true))
				.buildClassAndCreateNewInstance();

		List<StringHolder> strings = Arrays.asList(new StringHolder(null, "b"), new StringHolder(null, "a"),
				new StringHolder("b", null), new StringHolder("c", "e"),
				new StringHolder("c", "d"), new StringHolder(null, null), new StringHolder("d", "z"),
				new StringHolder(null, null));
		List<StringHolder> strings2 = new ArrayList<>(strings);
		strings.sort(generatedComparator);
		strings2.sort(new StringHolderComparator());

		assertEquals(strings, strings2);
	}

	public interface TestInterface {
		double returnDouble();
	}

	public static abstract class TestAbstract implements TestInterface {
		protected abstract int returnInt();
	}

	@org.junit.Test
	public void testAbstractClassWithInterface() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		TestAbstract testObj = ClassBuilder.create(classLoader, TestAbstract.class)
				.withMethod("returnInt", value(42))
				.withMethod("returnDouble", value(-1.0))
				.buildClassAndCreateNewInstance();
		assertEquals(42, testObj.returnInt());
		assertEquals(-1.0, testObj.returnDouble(), 1E-5);
	}

	public static abstract class A {
		public int t() {
			return 40;
		}

		public abstract int a();
	}

	public interface B {
		Integer b();
	}

	public interface C {
		String c();
	}

	@org.junit.Test
	public void testMultipleInterfacesWithAbstract() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		A instance = ClassBuilder.create(definingClassLoader, A.class, asList(B.class, C.class))
				.withMethod("a", value(42))
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.buildClassAndCreateNewInstance();

		assertEquals(40, instance.t());
		assertEquals(42, instance.a());
		assertEquals(Integer.valueOf(43), ((B) instance).b());
		assertEquals("44", ((C) instance).c());
	}

	@org.junit.Test
	public void testMultipleInterfaces() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		B instance = ClassBuilder.create(definingClassLoader, B.class, singletonList(C.class))
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.buildClassAndCreateNewInstance();

		assertEquals(instance.b(), Integer.valueOf(43));
		assertEquals("44", ((C) instance).c());

	}

	@org.junit.Test
	public void testNullableToString() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		B instance = ClassBuilder.create(definingClassLoader, B.class)
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						ExpressionToString.create()
								.withQuotes("{", "}", ", ")
								.with(call(self(), "b")))
				.buildClassAndCreateNewInstance();

		assertNull(instance.b());
		assertEquals("{null}", instance.toString());
	}

	@org.junit.Test
	public void testSetSaveBytecode() throws IOException {
		File folder = tempFolder.newFolder();
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		B instance = ClassBuilder.create(definingClassLoader, B.class)
				.withBytecodeSaveDir(folder.toPath())
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						ExpressionToString.create()
								.withQuotes("{", "}", ", ")
								.with(call(self(), "b")))
				.buildClassAndCreateNewInstance();
		assertEquals(1, folder.list().length );
		assertNull(instance.b());
		assertEquals("{null}", instance.toString());
	}

	public interface TestArraySet {
		Integer[] ints(Integer[] ints);
	}

	@org.junit.Test
	public void testArraySet() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		TestArraySet instance = ClassBuilder.create(definingClassLoader, TestArraySet.class)
				.withMethod("ints", sequence(arraySet(arg(0), value(0), cast(value(42), Integer.class)), arg(0)))
				.buildClassAndCreateNewInstance();
		Integer[] ints = {1, 2, 3, 4};

		assertArrayEquals(instance.ints(ints), new Integer[]{42, 2, 3, 4});
	}

	public interface TestCallStatic {
		int method(int a, int b);
	}

	@org.junit.Test
	public void testCallStatic() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		TestCallStatic instance = ClassBuilder.create(definingClassLoader, TestCallStatic.class)
				.withMethod("method", staticCall(Math.class, "min", arg(0), arg(1)))
				.buildClassAndCreateNewInstance();
		assertEquals(0, instance.method(5, 0));
		assertEquals(5, instance.method(5, 10));
	}

	public interface TestIsNull {
		boolean method(String a);
	}

	@org.junit.Test
	public void testIsNull() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		TestIsNull instance = ClassBuilder.create(definingClassLoader, TestIsNull.class)
				.withMethod("method", isNull(arg(0)))
				.buildClassAndCreateNewInstance();
		assertFalse(instance.method("42"));
		assertTrue(instance.method(null));
	}

	public interface TestIsNotNull {
		boolean method(Object a);
	}

	@org.junit.Test
	public void testIsNotNull() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		TestIsNotNull instance = ClassBuilder.create(definingClassLoader, TestIsNotNull.class)
				.withMethod("method", isNotNull(arg(0)))
				.buildClassAndCreateNewInstance();
		assertTrue(instance.method("42"));
		assertTrue(instance.method(42));
		assertFalse(instance.method(null));
	}

	public interface TestNewArray {
		int[] ints(int size);

		String[] integers(int size);
	}

	@org.junit.Test
	public void testNewArray() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		TestNewArray instance = ClassBuilder.create(definingClassLoader, TestNewArray.class)
				.withMethod("ints", arrayNew(int[].class, arg(0)))
				.withMethod("integers", arrayNew(String[].class, arg(0)))
				.buildClassAndCreateNewInstance();
		assertEquals(1, instance.ints(1).length);
		assertEquals(2, instance.integers(2).length);
	}

	@org.junit.Test
	public void testStaticConstants() {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		Object testObject = new Object();
		Getter instance = ClassBuilder.create(definingClassLoader, Getter.class)
				.withMethod("get", value(testObject))
				.buildClassAndCreateNewInstance();
		assertSame(testObject, instance.get(null));
	}

	@org.junit.Test
	public void testStaticFields() throws IllegalAccessException, InstantiationException {
		Class<StaticPojo> build = ClassBuilder.create(DefiningClassLoader.create(), StaticPojo.class)
				.withMethod("getField", staticField(StaticFieldHolder.class, "field"))
				.withMethod("setField", set(staticField(StaticFieldHolder.class, "field"), arg(0)))
				.build();
		StaticPojo staticPojo = build.newInstance();
		assertEquals(0, staticPojo.getField());
		staticPojo.setField(100);
		assertEquals(100, staticPojo.getField());
		staticPojo.setField(-100);
		assertEquals(-100, staticPojo.getField());
	}

	public interface StaticPojo {
		int getField();

		void setField(int value);
	}

	public static class StaticFieldHolder {
		public static int field;
	}
}
