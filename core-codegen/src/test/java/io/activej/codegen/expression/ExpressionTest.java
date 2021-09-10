package io.activej.codegen.expression;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.operation.ArithmeticOperation;
import io.activej.codegen.operation.CompareOperation;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static io.activej.codegen.TestUtils.assertStaticConstantsCleared;
import static io.activej.codegen.expression.ExpressionComparator.leftProperty;
import static io.activej.codegen.expression.ExpressionComparator.rightProperty;
import static io.activej.codegen.expression.Expressions.*;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument"})
public class ExpressionTest {
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@SuppressWarnings("unused")
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
		public final String property1;
		public final int property2;
		public final long property3;
		public final float property4;
		public final int property5;
		public final double property6;
		public final String property7;

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
			result = 31 * result + (property4 != 0.0f ? Float.floatToIntBits(property4) : 0);
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

	@SuppressWarnings("unused")
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
	public void test() throws ReflectiveOperationException {
		Class<Test> testClass = ClassBuilder.create(Test.class)
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
				.defineClass(CLASS_LOADER);
		Test test = testClass.getDeclaredConstructor().newInstance();

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

		Test test1 = testClass.getDeclaredConstructor().newInstance();
		Test test2 = testClass.getDeclaredConstructor().newInstance();

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
	public void test2() throws ReflectiveOperationException {
		Class<Test2> testClass = ClassBuilder.create(Test2.class)
				.withMethod("hash",
						hash(
								property(arg(0), "property1"),
								property(arg(0), "property2"),
								property(arg(0), "property3"),
								property(arg(0), "property4"),
								property(arg(0), "property5"),
								property(arg(0), "property6"),
								property(arg(0), "property7")))
				.defineClass(CLASS_LOADER);

		Test2 test = testClass.getDeclaredConstructor().newInstance();
		TestPojo2 testPojo2 = new TestPojo2("randomString", 42, 666666, 43258.42342f, 54359878, 43252353278423.423468, "fhsduighrwqruqsd");

		assertEquals(testPojo2.hashCode(), test.hash(testPojo2));
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparator() {
		Comparator<TestPojo> comparator = ClassBuilder.create(Comparator.class)
				.withMethod("compare",
						compare(TestPojo.class, "property1", "property2"))
				.defineClassAndCreateInstance(CLASS_LOADER);
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

		TestNeg testClass = ClassBuilder.create(TestNeg.class)
				.withMethod("negBoolean", neg(value(true)))
				.withMethod("negShort", neg(value(s)))
				.withMethod("negByte", neg(value(b)))
				.withMethod("negChar", neg(value(c)))
				.withMethod("negInt", neg(value(i)))
				.withMethod("negLong", neg(value(l)))
				.withMethod("negFloat", neg(value(f)))
				.withMethod("negDouble", neg(value(d)))
				.defineClassAndCreateInstance(CLASS_LOADER);

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

		TestOperation testClass = ClassBuilder.create(TestOperation.class)
				.withMethod("remB", arithmeticOp(ArithmeticOperation.REM, value(b), value(20)))
				.withMethod("remS", arithmeticOp(ArithmeticOperation.REM, value(s), value(20)))
				.withMethod("remC", arithmeticOp(ArithmeticOperation.REM, value(c), value(20)))
				.withMethod("remI", arithmeticOp(ArithmeticOperation.REM, value(i), value(20)))
				.withMethod("remL", arithmeticOp(ArithmeticOperation.REM, value(l), value(20)))
				.withMethod("remF", arithmeticOp(ArithmeticOperation.REM, value(f), value(20)))
				.withMethod("remD", arithmeticOp(ArithmeticOperation.REM, value(d), value(20)))
				.defineClassAndCreateInstance(CLASS_LOADER);

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

		TestSH testClass = ClassBuilder.create(TestSH.class)
				.withMethod("shlInt", shl(value(b), value(i)))
				.withMethod("shlLong", shl(value(l), value(b)))
				.withMethod("shrInt", shr(value(b), value(i)))
				.withMethod("shrLong", shr(value(l), value(i)))
				.withMethod("ushrInt", ushr(value(b), value(i)))
				.defineClassAndCreateInstance(CLASS_LOADER);

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
		TestBitMask testClass = ClassBuilder.create(TestBitMask.class)
				.withMethod("andInt", bitAnd(value(2), value(4)))
				.withMethod("orInt", bitOr(value(2), value(4)))
				.withMethod("xorInt", bitXor(value(2), value(4)))
				.withMethod("andLong", bitAnd(value(2), value(4L)))
				.withMethod("orLong", bitOr(value((byte) 2), value(4L)))
				.withMethod("xorLong", bitXor(value(2L), value(4L)))
				.defineClassAndCreateInstance(CLASS_LOADER);

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
		TestCall testClass = ClassBuilder.create(TestCall.class)
				.withMethod("callOther1", call(self(), "method", arg(0)))
				.withMethod("callOther2", call(self(), "method"))
				.withMethod("method", int.class, asList(int.class), arg(0))
				.withMethod("method", long.class, asList(), value(-1L))
				.withMethod("callStatic1", int.class, asList(int.class, int.class), staticCallSelf("method", arg(0), arg(1)))
				.withMethod("callStatic2", long.class, asList(long.class), staticCallSelf("method", arg(0)))
				.withStaticMethod("method", int.class, asList(int.class, int.class), arg(1))
				.withStaticMethod("method", long.class, asList(long.class), arg(0))
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(100, testClass.callOther1(100));
		assertEquals(-1, testClass.callOther2());
		assertEquals(2, testClass.callStatic1(1, 2));
		assertEquals(3L, testClass.callStatic2(3L));
	}

	public interface TestArgument {
		Object array(WriteFirstElement w, Object[] arr);

		Object write(WriteFirstElement w, Object o);
	}

	@SuppressWarnings("unused")
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
		TestArgument testClass = ClassBuilder.create(TestArgument.class)
				.withMethod("array", call(arg(0), "writeFirst", arg(1)))
				.withMethod("write", call(arg(0), "write", arg(1)))
				.defineClassAndCreateInstance(CLASS_LOADER);

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

		WriteAllListElement testClass = ClassBuilder.create(WriteAllListElement.class)
				.withMethod("write",
						forEach(arg(0),
								it -> sequence(call(arg(1), "add", it), voidExp())))
				.withMethod("writeIter",
						forEach(arg(0),
								it -> sequence(call(arg(1), "add", it), voidExp())))
				.defineClassAndCreateInstance(CLASS_LOADER);

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

		WriteArrayElements testClass = ClassBuilder.create(WriteArrayElements.class)
				.withMethod("write", forEach(arg(0),
						it -> sequence(call(arg(1), "add", cast(it, Object.class)), voidExp())))
				.defineClassAndCreateInstance(CLASS_LOADER);

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
		CastPrimitive testClass = ClassBuilder.create(CastPrimitive.class)
				.withMethod("a", value(1))
				.defineClassAndCreateInstance(CLASS_LOADER);

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
		DefiningClassLoader classLoader = CLASS_LOADER;
		Initializable intHolder = ClassBuilder.create(Initializable.class)
				.withField("x", int.class)
				.withMethod("init", set(property(self(), "x"), value(42)))
				.defineClassAndCreateInstance(classLoader);

		intHolder.init();

		Getter getter = ClassBuilder.create(Getter.class)
				.withMethod("get", property(cast(arg(0), intHolder.getClass()), "x"))
				.defineClassAndCreateInstance(classLoader);

		assertEquals(42, getter.get(intHolder));
	}

	@org.junit.Test
	public void testBuiltInstance() {
		Class<?> testClass1 = CLASS_LOADER.ensureClass(
				ClassKey.of(Object.class, "TestKey"),
				() -> ClassBuilder.create(Object.class));

		Class<?> testClass2 = CLASS_LOADER.ensureClass(
				ClassKey.of(Object.class, "TestKey"),
				() -> ClassBuilder.create(Object.class));

		assertEquals(testClass1, testClass2);
	}

	public interface TestCompare {
		boolean compareObjectLE(Integer i1, Integer i2);

		boolean comparePrimitiveLE(int i1, int i2);

		boolean compareObjectEQ(Integer i1, Integer i2);

		boolean compareObjectNE(Integer i1, Integer i2);

	}

	@org.junit.Test
	public void testCompare() throws ReflectiveOperationException {
		Class<TestCompare> test1 = ClassBuilder.create(TestCompare.class)
				.withMethod("compareObjectLE", cmp(CompareOperation.LE, arg(0), arg(1)))
				.withMethod("comparePrimitiveLE", cmp(CompareOperation.LE, arg(0), arg(1)))
				.withMethod("compareObjectEQ", cmp(CompareOperation.EQ, arg(0), arg(1)))
				.withMethod("compareObjectNE", cmp(CompareOperation.NE, arg(0), arg(1)))
				.defineClass(CLASS_LOADER);

		TestCompare testCompare = test1.getDeclaredConstructor().newInstance();
		assertTrue(testCompare.compareObjectLE(5, 5));
		assertTrue(testCompare.comparePrimitiveLE(5, 6));
		assertTrue(testCompare.compareObjectEQ(5, 5));
		assertTrue(testCompare.compareObjectNE(5, -5));
	}

	public static class StringHolder {
		public final String string1;
		public final String string2;

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
		Comparator<StringHolder> generatedComparator = ClassBuilder.create(Comparator.class)
				.withMethod("compare", ExpressionComparator.create()
						.with(leftProperty(StringHolder.class, "string1"), rightProperty(StringHolder.class, "string1"), true)
						.with(leftProperty(StringHolder.class, "string2"), rightProperty(StringHolder.class, "string2"), true))
				.defineClassAndCreateInstance(CLASS_LOADER);

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
		TestAbstract testObj = ClassBuilder.create(TestAbstract.class)
				.withMethod("returnInt", value(42))
				.withMethod("returnDouble", value(-1.0))
				.defineClassAndCreateInstance(CLASS_LOADER);
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
		A instance = ClassBuilder.create(A.class, B.class, C.class)
				.withMethod("a", value(42))
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(40, instance.t());
		assertEquals(42, instance.a());
		assertEquals(Integer.valueOf(43), ((B) instance).b());
		assertEquals("44", ((C) instance).c());
	}

	@org.junit.Test
	public void testMultipleInterfaces() {
		B instance = ClassBuilder.create(B.class, C.class)
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(instance.b(), Integer.valueOf(43));
		assertEquals("44", ((C) instance).c());

	}

	@org.junit.Test
	public void testNullableToString() {
		B instance = ClassBuilder.create(B.class)
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						ExpressionToString.create()
								.withQuotes("{", "}", ", ")
								.with(call(self(), "b")))
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertNull(instance.b());
		assertEquals("{null}", instance.toString());
	}

	@org.junit.Test
	public void testSetSaveBytecode() throws IOException {
		File dir = temporaryFolder.newFolder();
		B instance = ClassBuilder.create(B.class)
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						ExpressionToString.create()
								.withQuotes("{", "}", ", ")
								.with(call(self(), "b")))
				.defineClassAndCreateInstance(DefiningClassLoader.create()
						.withDebugOutputDir(dir.toPath()));
		//noinspection ConstantConditions
		assertEquals(1, dir.list().length);
		assertNull(instance.b());
		assertEquals("{null}", instance.toString());
	}

	public interface TestArraySet {
		Integer[] ints(Integer[] ints);
	}

	@org.junit.Test
	public void testArraySet() {
		TestArraySet instance = ClassBuilder.create(TestArraySet.class)
				.withMethod("ints", sequence(arraySet(arg(0), value(0), cast(value(42), Integer.class)), arg(0)))
				.defineClassAndCreateInstance(CLASS_LOADER);
		Integer[] ints = {1, 2, 3, 4};

		assertArrayEquals(instance.ints(ints), new Integer[]{42, 2, 3, 4});
	}

	public interface TestCallStatic {
		int method(int a, int b);
	}

	@org.junit.Test
	public void testCallStatic() {
		TestCallStatic instance = ClassBuilder.create(TestCallStatic.class)
				.withMethod("method", staticCall(Math.class, "min", arg(0), arg(1)))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertEquals(0, instance.method(5, 0));
		assertEquals(5, instance.method(5, 10));
	}

	public interface TestIsNull {
		boolean method(String a);
	}

	@org.junit.Test
	public void testIsNull() {
		TestIsNull instance = ClassBuilder.create(TestIsNull.class)
				.withMethod("method", isNull(arg(0)))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertFalse(instance.method("42"));
		assertTrue(instance.method(null));
	}

	public interface TestIsNotNull {
		boolean method(Object a);
	}

	@org.junit.Test
	public void testIsNotNull() {
		TestIsNotNull instance = ClassBuilder.create(TestIsNotNull.class)
				.withMethod("method", isNotNull(arg(0)))
				.defineClassAndCreateInstance(CLASS_LOADER);
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
		TestNewArray instance = ClassBuilder.create(TestNewArray.class)
				.withMethod("ints", arrayNew(int[].class, arg(0)))
				.withMethod("integers", arrayNew(String[].class, arg(0)))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertEquals(1, instance.ints(1).length);
		assertEquals(2, instance.integers(2).length);
	}

	@org.junit.Test
	public void testStaticConstants() {
		ClassBuilder.clearStaticConstants();
		Object testObject = new Object();
		Getter instance = ClassBuilder.create(Getter.class)
				.withMethod("get", value(testObject))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertSame(testObject, instance.get(null));
		assertStaticConstantsCleared();
	}

	@org.junit.Test
	public void testFields() {
		ClassBuilder.clearStaticConstants();
		Object testObject = new Object();
		Getter instance = ClassBuilder.create(Getter.class)
				.withField("field1", Object.class, value(testObject))
				.withMethod("get", property(self(), "field1"))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertSame(testObject, instance.get(null));
		assertStaticConstantsCleared();
	}

	@org.junit.Test
	public void testStaticFields() throws ReflectiveOperationException {
		Class<StaticPojo> build = ClassBuilder.create(StaticPojo.class)
				.withStaticField("field", int.class, value(10))
				.withMethod("getField", staticField(StaticFieldHolder.class, "field"))
				.withMethod("setField", set(staticField(StaticFieldHolder.class, "field"), arg(0)))
				.defineClass(CLASS_LOADER);
		StaticPojo staticPojo = build.getDeclaredConstructor().newInstance();
		assertEquals(0, staticPojo.getField());
		staticPojo.setField(100);
		assertEquals(100, staticPojo.getField());
		staticPojo.setField(-100);
		assertEquals(-100, staticPojo.getField());
		staticPojo.setField(0);
	}

	public interface StaticPojo {
		int getField();

		void setField(int value);
	}

	public static class StaticFieldHolder {
		public static int field;
	}

	public interface ErrorThrower {
		void throwChecked(String message) throws Exception;

		void throwUnchecked();

		void throwCheckedWithSuppressed() throws Exception;
	}

	@org.junit.Test
	public void testExceptionThrowing() {
		ErrorThrower errorThrower = ClassBuilder.create(ErrorThrower.class)
				.withMethod("throwChecked", throwException(IOException.class, arg(0)))
				.withMethod("throwUnchecked", throwException(RuntimeException.class))
				.withMethod("throwCheckedWithSuppressed",
						throwException(let(constructor(Exception.class, value("main")), exception ->
								sequence(
										call(exception, "addSuppressed", constructor(Exception.class, value("first"))),
										call(exception, "addSuppressed", constructor(Exception.class, value("second"))),
										call(exception, "addSuppressed", constructor(Exception.class, value("third"))),
										exception
								))))
				.defineClassAndCreateInstance(CLASS_LOADER);

		try {
			errorThrower.throwChecked("Fail");
			fail();
		} catch (Exception e) {
			assertThat(e, instanceOf(IOException.class));
			assertEquals("Fail", e.getMessage());
		}

		try {
			errorThrower.throwUnchecked();
			fail();
		} catch (RuntimeException ignored) {
		}

		try {
			errorThrower.throwCheckedWithSuppressed();
			fail();
		} catch (Exception e) {
			assertEquals("main", e.getMessage());

			Throwable[] suppressed = e.getSuppressed();
			assertEquals(3, suppressed.length);
			assertEquals("first", suppressed[0].getMessage());
			assertEquals("second", suppressed[1].getMessage());
			assertEquals("third", suppressed[2].getMessage());
		}
	}

	@org.junit.Test
	public void testSuperMethods() {
		ClassBuilder.clearStaticConstants();
		Super instance = ClassBuilder.create(Super.class)
				.withMethod("getString", concat(value("super returns: "), callSuper("getString")))
				.withMethod("change", add(callSuper("change", arg(0)), value(100)))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertEquals("super returns: hello", instance.getString());
		assertEquals(150, instance.change(40));
		assertStaticConstantsCleared();
	}

	public static class Super {
		protected String getString() {
			return "hello";
		}

		public int change(int x) {
			return x + 10;
		}
	}

	public interface TestConcat {
		String concat(byte aByte, int anInt, String space, long aLong, char aChar, Object anObject, TestPojo testPojo);
	}

	@org.junit.Test
	public void testCallingOfProtectedMethods() {
		ClassBuilder.clearStaticConstants();
		Cashier instance = ClassBuilder.create(Cashier.class)
				.withMethod("getPrice", mul(Expressions.value(2), Expressions.call(self(), "hiddenPrice")))
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertEquals(200, instance.getPrice());
		assertStaticConstantsCleared();
	}

	public static class Cashier {

		protected final int hiddenPrice() {
			return 100;
		}

		public int getPrice() {
			return hiddenPrice() + 50;
		}
	}

	@SuppressWarnings("ConstantConditions")
	@org.junit.Test
	public void testConcat() {
		byte aByte = -123;
		int anInt = 124124211;
		String space = " ";
		long aLong = -1_000_000_000_000_000L;
		char aChar = 't';
		Object anObject = null;
		TestPojo testPojo = new TestPojo(10, 20);

		TestConcat testConcat = ClassBuilder.create(TestConcat.class)
				.withMethod("concat", concat(arg(0), arg(1), arg(2),
						arg(3), arg(4), arg(5), arg(6)))
				.defineClassAndCreateInstance(CLASS_LOADER);

		String expected = "" + aByte + anInt + space + aLong + aChar + anObject + testPojo;
		String actual = testConcat.concat(aByte, anInt, space, aLong, aChar, anObject, testPojo);
		assertEquals(expected, actual);
	}
}
