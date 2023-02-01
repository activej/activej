package io.activej.codegen.expression;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.impl.Expression_Compare;
import io.activej.codegen.expression.impl.Expression_HashCode;
import io.activej.codegen.expression.impl.Expression_ToString;
import io.activej.codegen.operation.ArithmeticOperation;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static io.activej.codegen.TestUtils.assertStaticConstantsCleared;
import static io.activej.codegen.expression.Expressions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

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

	public record TestPojo2(String property1, int property2, long property3, float property4, int property5,
							double property6, String property7) {
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
		int compareTo(Test o);

		@Override
		String toString();

		void loop();
	}

	@org.junit.Test
	public void test() throws ReflectiveOperationException {
		Class<Test> testClass = ClassBuilder.builder(Test.class)
				.withField("x", int.class)
				.withField("y", Long.class)
				.withMethod("compare", int.class, List.of(TestPojo.class, TestPojo.class),
						Expression_Compare.builder()
								.with(leftProperty(TestPojo.class, "property1"), rightProperty(TestPojo.class, "property1"))
								.with(leftProperty(TestPojo.class, "property2"), rightProperty(TestPojo.class, "property2"))
								.build())
				.withMethod("int compareTo(io.activej.codegen.expression.ExpressionTest$Test)",
						comparableImpl("x"))
				.withMethod("equals",
						equalsImpl("x"))
				.withMethod("setXY", sequence(
						set(property(self(), "x"), arg(0)),
						set(property(self(), "y"), arg(1))))
				.withMethod("test",
						add(arg(0), value(1L)))
				.withMethod("hash",
						Expression_HashCode.builder()
								.with(property(arg(0), "property1"))
								.with(property(arg(0), "property2"))
								.build())
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
						and(isEq(arg(0), arg(1)), isEq(arg(0), arg(2))))
				.withMethod("anyEqual",
						or(isEq(arg(0), arg(1)), isEq(arg(0), arg(2))))
				.withMethod("setPojoproperty1",
						call(arg(0), "setproperty1", arg(1)))
				.withMethod("getPojoproperty1",
						call(arg(0), "getproperty1"))
				.withMethod("toString",
						Expression_ToString.builder()
								.withField("x")
								.with(value("test"))
								.with("labelY", property(self(), "y"))
								.build())
				.build()
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

		assertEquals("{x: 1, test, labelY: 10}", test1.toString());
	}

	@org.junit.Test
	public void test2() throws ReflectiveOperationException {
		Class<Test2> testClass = ClassBuilder.builder(Test2.class)
				.withMethod("hash",
						Expression_HashCode.builder()
								.with(property(arg(0), "property1"))
								.with(property(arg(0), "property2"))
								.with(property(arg(0), "property3"))
								.with(property(arg(0), "property4"))
								.with(property(arg(0), "property5"))
								.with(property(arg(0), "property6"))
								.with(property(arg(0), "property7"))
								.build())
				.build()
				.defineClass(CLASS_LOADER);

		Test2 test = testClass.getDeclaredConstructor().newInstance();
		TestPojo2 testPojo2 = new TestPojo2("randomString", 42, 666666, 43258.42342f, 54359878, 43252353278423.423468, "fhsduighrwqruqsd");

		assertEquals(testPojo2.hashCode(), test.hash(testPojo2));
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparator() {
		Comparator<TestPojo> comparator = ClassBuilder.builder(Comparator.class)
				.withMethod("compare",
						Expression_Compare.builder()
								.with(leftProperty(TestPojo.class, "property1"), rightProperty(TestPojo.class, "property1"))
								.with(leftProperty(TestPojo.class, "property2"), rightProperty(TestPojo.class, "property2"))
								.build())
				.build()
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

		TestNeg testNeg = ClassBuilder.builder(TestNeg.class)
				.withMethod("negBoolean", neg(value(true)))
				.withMethod("negShort", neg(value(s)))
				.withMethod("negByte", neg(value(b)))
				.withMethod("negChar", neg(value(c)))
				.withMethod("negInt", neg(value(i)))
				.withMethod("negLong", neg(value(l)))
				.withMethod("negFloat", neg(value(f)))
				.withMethod("negDouble", neg(value(d)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertFalse(testNeg.negBoolean());
		assertEquals(testNeg.negShort(), -s);
		assertEquals(testNeg.negByte(), -b);
		assertEquals(testNeg.negChar(), -c);
		assertEquals(testNeg.negInt(), -i);
		assertEquals(testNeg.negLong(), -l);
		assertEquals(testNeg.negFloat(), -f, 0.0);
		assertEquals(testNeg.negDouble(), -d, 0.0);
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

		TestOperation testOp = ClassBuilder.builder(TestOperation.class)
				.withMethod("remB", arithmeticOp(ArithmeticOperation.REM, value(b), value(20)))
				.withMethod("remS", arithmeticOp(ArithmeticOperation.REM, value(s), value(20)))
				.withMethod("remC", arithmeticOp(ArithmeticOperation.REM, value(c), value(20)))
				.withMethod("remI", arithmeticOp(ArithmeticOperation.REM, value(i), value(20)))
				.withMethod("remL", arithmeticOp(ArithmeticOperation.REM, value(l), value(20)))
				.withMethod("remF", arithmeticOp(ArithmeticOperation.REM, value(f), value(20)))
				.withMethod("remD", arithmeticOp(ArithmeticOperation.REM, value(d), value(20)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(testOp.remB(), b % 20);
		assertEquals(testOp.remS(), s % 20);
		assertEquals(testOp.remC(), c % 20);
		assertEquals(testOp.remI(), i % 20);
		assertEquals(testOp.remL(), l % 20);
		assertEquals(testOp.remF(), f % 20, 0.0);
		assertEquals(testOp.remD(), d % 20, 0.0);
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

		TestSH testSh = ClassBuilder.builder(TestSH.class)
				.withMethod("shlInt", shl(value(b), value(i)))
				.withMethod("shlLong", shl(value(l), value(b)))
				.withMethod("shrInt", shr(value(b), value(i)))
				.withMethod("shrLong", shr(value(l), value(i)))
				.withMethod("ushrInt", ushr(value(b), value(i)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(testSh.shlInt(), b << i);
		assertEquals(testSh.shlLong(), l << b);
		assertEquals(testSh.shrInt(), b >> i);
		assertEquals(testSh.shrLong(), l >> i);
		assertEquals(testSh.ushrInt(), b >>> i);
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
		TestBitMask testBitMask = ClassBuilder.builder(TestBitMask.class)
				.withMethod("andInt", bitAnd(value(2), value(4)))
				.withMethod("orInt", bitOr(value(2), value(4)))
				.withMethod("xorInt", bitXor(value(2), value(4)))
				.withMethod("andLong", bitAnd(value(2), value(4L)))
				.withMethod("orLong", bitOr(value((byte) 2), value(4L)))
				.withMethod("xorLong", bitXor(value(2L), value(4L)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(testBitMask.andInt(), 2 & 4);
		assertEquals(testBitMask.orInt(), 2 | 4);
		assertEquals(testBitMask.xorInt(), 2 ^ 4);
		assertEquals(testBitMask.andLong(), 2L & 4L);
		assertEquals(testBitMask.orLong(), 2L | 4L);
		assertEquals(testBitMask.xorLong(), 2L ^ 4L);
	}

	public interface TestCall {
		int callOther1(int i);

		long callOther2();

		int callStatic1(int i1, int i2);

		long callStatic2(long l);
	}

	@org.junit.Test
	public void testCall() {
		TestCall testCall = ClassBuilder.builder(TestCall.class)
				.withMethod("callOther1", call(self(), "method", arg(0)))
				.withMethod("callOther2", call(self(), "method"))
				.withMethod("method", int.class, List.of(int.class), arg(0))
				.withMethod("method", long.class, List.of(), value(-1L))
				.withMethod("callStatic1", int.class, List.of(int.class, int.class), staticCallSelf("method", arg(0), arg(1)))
				.withMethod("callStatic2", long.class, List.of(long.class), staticCallSelf("method", arg(0)))
				.withStaticMethod("method", int.class, List.of(int.class, int.class), arg(1))
				.withStaticMethod("method", long.class, List.of(long.class), arg(0))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(100, testCall.callOther1(100));
		assertEquals(-1, testCall.callOther2());
		assertEquals(2, testCall.callStatic1(1, 2));
		assertEquals(3L, testCall.callStatic2(3L));
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
		TestArgument testArg = ClassBuilder.builder(TestArgument.class)
				.withMethod("array", call(arg(0), "writeFirst", arg(1)))
				.withMethod("write", call(arg(0), "write", arg(1)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(1000, testArg.array(new WriteFirstElement(), new Object[]{1000, 2, 3, 4}));
		assertEquals(1000, testArg.write(new WriteFirstElement(), 1000));
	}

	public interface WriteAllListElement {
		void write(List<?> listFrom, List<?> listTo);

		void writeIter(Iterator<?> iteratorFrom, List<?> listTo);
	}

	@org.junit.Test
	public void testIterator() {
		List<Integer> listFrom = List.of(1, 1, 2, 3, 5, 8);
		List<Integer> listTo1 = new ArrayList<>();
		List<Integer> listTo2 = new ArrayList<>();

		WriteAllListElement writeAllListElement = ClassBuilder.builder(WriteAllListElement.class)
				.withMethod("write",
						iterateIterable(arg(0), it -> call(arg(1), "add", it)))
				.withMethod("writeIter",
						iterateIterator(arg(0),
								it -> call(arg(1), "add", it)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		writeAllListElement.write(listFrom, listTo1);
		writeAllListElement.writeIter(listFrom.iterator(), listTo2);

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

		WriteArrayElements writeArrayElements = ClassBuilder.builder(WriteArrayElements.class)
				.withMethod("write", iterateArray(arg(0),
						it -> call(arg(1), "add", cast(it, Object.class))))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		writeArrayElements.write(intsFrom, list);
		for (int i = 0; i < intsFrom.length; i++) {
			assertEquals(intsFrom[i], list.get(i));
		}
	}

	public interface CastPrimitive {
		Object a();
	}

	@org.junit.Test
	public void testCastPrimitive() {
		CastPrimitive castPrimitive = ClassBuilder.builder(CastPrimitive.class)
				.withMethod("a", value(1))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(1, castPrimitive.a());
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
		Initializable intHolder = ClassBuilder.builder(Initializable.class)
				.withField("x", int.class)
				.withMethod("init", set(property(self(), "x"), value(42)))
				.build()
				.defineClassAndCreateInstance(classLoader);

		intHolder.init();

		Getter getter = ClassBuilder.builder(Getter.class)
				.withMethod("get", property(cast(arg(0), intHolder.getClass()), "x"))
				.build()
				.defineClassAndCreateInstance(classLoader);

		assertEquals(42, getter.get(intHolder));
	}

	@org.junit.Test
	public void testBuiltInstance() {
		Class<?> testClass1 = CLASS_LOADER.ensureClass(
				ClassKey.of(Object.class, "TestKey"),
				() -> ClassBuilder.builder(Object.class).build());

		Class<?> testClass2 = CLASS_LOADER.ensureClass(
				ClassKey.of(Object.class, "TestKey"),
				() -> ClassBuilder.builder(Object.class).build());

		assertEquals(testClass1, testClass2);
	}

	public interface TestCompare {
		boolean compareObjectLE(Integer i1, Integer i2);

		boolean compareInterfaceGE(TestInterface ti1, TestInterface ti2);

		boolean comparePrimitiveLE(int i1, int i2);

		boolean compareObjectEQ(Integer i1, Integer i2);

		boolean compareInterfaceEQ(List<Integer> l1, List<Integer> l2);

		boolean compareObjectNE(Integer i1, Integer i2);

		boolean compareInterfaceNE(TestInterface ti1, TestInterface ti2);

	}

	@org.junit.Test
	public void testCompare() throws ReflectiveOperationException {
		Class<TestCompare> test1 = ClassBuilder.builder(TestCompare.class)
				.withMethod("compareObjectLE", isLe(arg(0), arg(1)))
				.withMethod("comparePrimitiveLE", isLe(arg(0), arg(1)))
				.withMethod("compareObjectEQ", isEq(arg(0), arg(1)))
				.withMethod("compareObjectNE", isNe(arg(0), arg(1)))
				.withMethod("compareInterfaceEQ", isEq(arg(0), arg(1)))
				.withMethod("compareInterfaceNE", isNe(arg(0), arg(1)))
				.withMethod("compareInterfaceGE", isGe(arg(0), arg(1)))
				.build()
				.defineClass(CLASS_LOADER);

		TestCompare testCompare = test1.getDeclaredConstructor().newInstance();
		assertTrue(testCompare.compareObjectLE(5, 5));
		assertTrue(testCompare.comparePrimitiveLE(5, 6));
		assertTrue(testCompare.compareObjectEQ(5, 5));
		assertTrue(testCompare.compareObjectNE(5, -5));
		assertTrue(testCompare.compareInterfaceEQ(Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3)));
		assertFalse(testCompare.compareInterfaceNE(new TestInterfaceImpl(3.4), new TestInterfaceImpl(3.4)));
		assertTrue(testCompare.compareInterfaceGE(new TestInterfaceImpl(5.4), new TestInterfaceImpl(3.4)));
	}

	public record StringHolder(String string1, String string2) {}

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

	public static class InterfaceHolderComparator implements Comparator<InterfaceHolder> {
		public int compare(InterfaceHolder var1, InterfaceHolder var2) {
			TestInterface var1Interface1 = var1.getInterface1();
			TestInterface var2Interface1 = var2.getInterface1();
			int compare;
			if (var1Interface1 == null) {
				if (var2Interface1 != null) {
					return -1;
				}
			} else {
				if (var2Interface1 == null) {
					return 1;
				}

				compare = var1Interface1.compareTo(var2Interface1);
				if (compare != 0) {
					return compare;
				}
			}

			TestInterface var1Interface2 = var1.getInterface2();
			TestInterface var2Interface2 = var2.getInterface2();

			return var1Interface2.compareTo(var2Interface2);

		}
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparatorNullable() {
		Comparator<StringHolder> generatedComparator = ClassBuilder.builder(Comparator.class)
				.withMethod("compare", Expression_Compare.builder()
						.with(leftProperty(StringHolder.class, "string1"), rightProperty(StringHolder.class, "string1"), true)
						.with(leftProperty(StringHolder.class, "string2"), rightProperty(StringHolder.class, "string2"), true)
						.build())
				.build()
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

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparatorInterface() {
		Comparator<InterfaceHolder> generatedComparator = ClassBuilder.builder(Comparator.class)
				.withMethod("compare", Expression_Compare.builder()
						.with(leftProperty(InterfaceHolder.class, "interface1"), rightProperty(InterfaceHolder.class, "interface1"), true)
						.with(leftProperty(InterfaceHolder.class, "interface2"), rightProperty(InterfaceHolder.class, "interface2"), false)
						.build())
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		List<InterfaceHolder> interfaceHolders = Arrays.asList(
				new InterfaceHolder(null, new TestInterfaceImpl(2.3)),
				new InterfaceHolder(null, new TestInterfaceImpl(1.2)),
				new InterfaceHolder(new TestInterfaceImpl(3.5), new TestInterfaceImpl(5.7)),
				new InterfaceHolder(new TestInterfaceImpl(3.5), new TestInterfaceImpl(6.8)),
				new InterfaceHolder(new TestInterfaceImpl(6.8), new TestInterfaceImpl(10.4))
		);
		List<InterfaceHolder> interfaceHolders2 = new ArrayList<>(interfaceHolders);
		interfaceHolders.sort(generatedComparator);
		interfaceHolders2.sort(new InterfaceHolderComparator());

		assertEquals(interfaceHolders, interfaceHolders2);
	}

	public interface TestInterface extends Comparable<TestInterface> {
		double returnDouble();
	}

	public interface TestInterfaceWrapper {
		TestInterface getTestInterface();
	}

	public static abstract class TestAbstract implements TestInterface {
		protected abstract int returnInt();
	}

	public static class TestInterfaceImpl implements TestInterface {
		private final double value;

		public TestInterfaceImpl(double value) {
			this.value = value;
		}

		@Override
		public double returnDouble() {
			return value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestInterfaceImpl that = (TestInterfaceImpl) o;
			return Double.compare(that.value, value) == 0;
		}

		@Override
		public int hashCode() {
			return Objects.hash(value);
		}

		@Override
		public int compareTo(ExpressionTest.TestInterface o) {
			return Double.compare(returnDouble(), o.returnDouble());
		}

		@Override
		public String toString() {
			return "TestInterfaceImpl{" +
					"value=" + value +
					'}';
		}
	}

	public static class InterfaceHolder {
		private final TestInterface interface1;
		private final TestInterface interface2;

		public InterfaceHolder(@Nullable TestInterface interface1, TestInterface interface2) {
			this.interface1 = interface1;
			this.interface2 = interface2;
		}

		@Nullable
		public TestInterface getInterface1() {
			return interface1;
		}

		public TestInterface getInterface2() {
			return interface2;
		}
	}

	@org.junit.Test
	public void testAbstractClassWithInterface() {
		TestAbstract testObj = ClassBuilder.builder(TestAbstract.class)
				.withMethod("returnInt", value(42))
				.withMethod("returnDouble", value(-1.0))
				.build()
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
		A instance = ClassBuilder.builder(A.class, B.class, C.class)
				.withMethod("a", value(42))
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(40, instance.t());
		assertEquals(42, instance.a());
		assertEquals(Integer.valueOf(43), ((B) instance).b());
		assertEquals("44", ((C) instance).c());
	}

	@org.junit.Test
	public void testMultipleInterfaces() {
		B instance = ClassBuilder.builder(B.class, C.class)
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(instance.b(), Integer.valueOf(43));
		assertEquals("44", ((C) instance).c());

	}

	@org.junit.Test
	public void testNullableToString() {
		B instance = ClassBuilder.builder(B.class)
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						Expression_ToString.builder()
								.with(call(self(), "b"))
								.build())
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertNull(instance.b());
		assertEquals("{null}", instance.toString());
	}

	@org.junit.Test
	public void testInterfaceToString() {
		TestInterfaceImpl value = new TestInterfaceImpl(3.5);
		TestInterfaceWrapper wrapper = ClassBuilder.builder(TestInterfaceWrapper.class)
				.withMethod("getTestInterface", value(value))
				.withMethod("toString",
						Expression_ToString.builder()
								.with(call(self(), "getTestInterface"))
								.build())
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(value, wrapper.getTestInterface());
		assertEquals("{" + value + "}", wrapper.toString());
	}

	@org.junit.Test
	public void testSetSaveBytecode() throws IOException {
		File dir = temporaryFolder.newFolder();
		B instance = ClassBuilder.builder(B.class)
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						Expression_ToString.builder()
								.with(call(self(), "b"))
								.build())
				.build()
				.defineClassAndCreateInstance(DefiningClassLoader.builder()
						.withDebugOutputDir(dir.toPath())
						.build());
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
		TestArraySet instance = ClassBuilder.builder(TestArraySet.class)
				.withMethod("ints", sequence(arraySet(arg(0), value(0), cast(value(42), Integer.class)), arg(0)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		Integer[] ints = {1, 2, 3, 4};

		assertArrayEquals(instance.ints(ints), new Integer[]{42, 2, 3, 4});
	}

	public interface TestCallStatic {
		int method(int a, int b);
	}

	@org.junit.Test
	public void testCallStatic() {
		TestCallStatic instance = ClassBuilder.builder(TestCallStatic.class)
				.withMethod("method", staticCall(Math.class, "min", arg(0), arg(1)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertEquals(0, instance.method(5, 0));
		assertEquals(5, instance.method(5, 10));
	}

	public static final class TestCallStaticSelf {
		public static int method(Object argument) {
			return 0;
		}

		public static int method(Self1 argument) {
			return 1;
		}
	}

	public static final class TestCallStaticSelfAmbiguous {
		public static int method(Self1 argument) {
			return 1;
		}

		public static int method(Self2 argument) {
			return 2;
		}
	}

	public interface Self1 {
		int getValue1();
	}

	public interface Self2 {
		int getValue2();
	}

	@org.junit.Test
	public void testCallStaticWithSelfArgument() {
		Self1 instance = ClassBuilder.builder(Self1.class)
				.withMethod("getValue1", staticCall(TestCallStaticSelf.class, "method", self()))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(1, instance.getValue1());
	}

	@org.junit.Test
	public void testCallStaticWithCastSelfArgument() {
		Self1 instance = ClassBuilder.builder(Self1.class)
				.withMethod("getValue1", staticCall(TestCallStaticSelf.class, "method", cast(self(), Object.class)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(0, instance.getValue1());
	}

	@org.junit.Test
	public void testCallStaticWithAmbiguousSelfArgument() {
		ClassBuilder<Self1> classBuilder = ClassBuilder.builder(Self1.class, Self2.class)
				.withMethod("getValue1", staticCall(TestCallStaticSelfAmbiguous.class, "method", self()))
				.withMethod("getValue2", staticCall(TestCallStaticSelfAmbiguous.class, "method", self()))
				.build();
		try {
			classBuilder.defineClassAndCreateInstance(CLASS_LOADER);
			fail();
		} catch (IllegalArgumentException e) {
			assertTrue(e.getMessage().startsWith("Ambiguous method: "));
		}
	}

	@org.junit.Test
	public void testCallStaticWithCastAmbiguousSelfArgument() {
		Self1 instance = ClassBuilder.builder(Self1.class, Self2.class)
				.withMethod("getValue1", staticCall(TestCallStaticSelfAmbiguous.class, "method", cast(self(), Self1.class)))
				.withMethod("getValue2", staticCall(TestCallStaticSelfAmbiguous.class, "method", cast(self(), Self2.class)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		assertEquals(1, instance.getValue1());
		assertEquals(2, ((Self2) instance).getValue2());
	}

	public interface TestIsNull {
		boolean method(String a);
	}

	@org.junit.Test
	public void testIsNull() {
		TestIsNull instance = ClassBuilder.builder(TestIsNull.class)
				.withMethod("method", isNull(arg(0)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertFalse(instance.method("42"));
		assertTrue(instance.method(null));
	}

	public interface TestIsNotNull {
		boolean method(Object a);
	}

	@org.junit.Test
	public void testIsNotNull() {
		TestIsNotNull instance = ClassBuilder.builder(TestIsNotNull.class)
				.withMethod("method", isNotNull(arg(0)))
				.build()
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
		TestNewArray instance = ClassBuilder.builder(TestNewArray.class)
				.withMethod("ints", arrayNew(int[].class, arg(0)))
				.withMethod("integers", arrayNew(String[].class, arg(0)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertEquals(1, instance.ints(1).length);
		assertEquals(2, instance.integers(2).length);
	}

	@org.junit.Test
	public void testStaticConstants() {
		ClassBuilder.clearStaticConstants();
		Object testObject = new Object();
		Getter instance = ClassBuilder.builder(Getter.class)
				.withMethod("get", value(testObject))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertSame(testObject, instance.get(null));
		assertStaticConstantsCleared();
	}

	@org.junit.Test
	public void testFields() {
		ClassBuilder.clearStaticConstants();
		Object testObject = new Object();
		Getter instance = ClassBuilder.builder(Getter.class)
				.withField("field1", Object.class, value(testObject))
				.withMethod("get", property(self(), "field1"))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);
		assertSame(testObject, instance.get(null));
		assertStaticConstantsCleared();
	}

	@org.junit.Test
	public void testStaticFields() throws ReflectiveOperationException {
		Class<StaticPojo> build = ClassBuilder.builder(StaticPojo.class)
				.withStaticField("field", int.class, value(10))
				.withMethod("getField", staticField(StaticFieldHolder.class, "field"))
				.withMethod("setField", set(staticField(StaticFieldHolder.class, "field"), arg(0)))
				.build()
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
		ErrorThrower errorThrower = ClassBuilder.builder(ErrorThrower.class)
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
				.build()
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
		Super instance = ClassBuilder.builder(Super.class)
				.withMethod("getString", concat(value("super returns: "), callSuper("getString")))
				.withMethod("change", add(callSuper("change", arg(0)), value(100)))
				.build()
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
		String concat(byte aByte, int anInt, String space, long aLong, char aChar, Object anObject, TestPojo testPojo, TestInterface testInterface);
	}

	@org.junit.Test
	public void testCallingOfProtectedMethods() {
		ClassBuilder.clearStaticConstants();
		Cashier instance = ClassBuilder.builder(Cashier.class)
				.withMethod("getPrice", mul(value(2), call(self(), "hiddenPrice")))
				.build()
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
		TestInterface testInterface = new TestInterfaceImpl(4.3);

		TestConcat testConcat = ClassBuilder.builder(TestConcat.class)
				.withMethod("concat", concat(arg(0), arg(1), arg(2),
						arg(3), arg(4), arg(5), arg(6), arg(7)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		String expected = "" + aByte + anInt + space + aLong + aChar + anObject + testPojo + testInterface;
		String actual = testConcat.concat(aByte, anInt, space, aLong, aChar, anObject, testPojo, testInterface);
		assertEquals(expected, actual);
	}

	@org.junit.Test
	public void testSequenceWithThrow() {
		TestSeq testSeq = ClassBuilder.builder(TestSeq.class)
				.withMethod("seq", sequence(throwException(RuntimeException.class, "test")))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		Ref ref = new Ref();
		try {
			testSeq.seq(ref);
		} catch (RuntimeException e) {
			assertEquals("test", e.getMessage());
		}
		assertNull(ref.value);
	}

	@org.junit.Test
	public void testSequenceWithThrowAndRef() {
		TestSeq testSeq = ClassBuilder.builder(TestSeq.class)
				.withMethod("seq", sequence(
						set(property(arg(0), "value"), value(1)),
						set(property(arg(0), "value"), value(2)),
						throwException(RuntimeException.class, "test")))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		Ref ref = new Ref();
		try {
			testSeq.seq(ref);
		} catch (RuntimeException e) {
			assertEquals("test", e.getMessage());
		}
		assertEquals(2, ref.value);
	}

	@org.junit.Test
	public void testIterateWithThrow() {
		TestIterate testIterate = ClassBuilder.builder(TestIterate.class)
				.withMethod("iterate", iterate(
						value(0),
						value(10),
						$ -> throwException(RuntimeException.class, "test")))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		Ref ref = new Ref();
		try {
			testIterate.iterate(ref);
		} catch (RuntimeException e) {
			assertEquals("test", e.getMessage());
		}
		assertNull(ref.value);
	}

	@org.junit.Test
	public void testIterateWithThrowAndRef() {
		TestIterate testIterate = ClassBuilder.builder(TestIterate.class)
				.withMethod("iterate", iterate(
						value(0),
						value(10),
						idx -> ifEq(
								idx, value(5),
								throwException(RuntimeException.class, "test"),
								set(property(arg(0), "value"), idx)
						)))
				.build()
				.defineClassAndCreateInstance(CLASS_LOADER);

		Ref ref = new Ref();
		try {
			testIterate.iterate(ref);
		} catch (RuntimeException e) {
			assertEquals("test", e.getMessage());
		}
		assertEquals(4, ref.value);
	}

	@org.junit.Test
	public void testConstructorWithThrow() throws NoSuchMethodException, IllegalAccessException, InstantiationException {
		Class<TestIterate> testIterateCls = ClassBuilder.builder(TestIterate.class)
				.withConstructor(List.of(Ref.class), throwException(RuntimeException.class, "test"))
				.withMethod("iterate", throwException(new AssertionError()))
				.build()
				.defineClass(CLASS_LOADER);

		Ref ref = new Ref();
		try {
			testIterateCls.getConstructor(Ref.class).newInstance(ref);
		} catch (InvocationTargetException e) {
			Throwable cause = e.getCause();
			assertSame(RuntimeException.class, cause.getClass());
			assertEquals("test", cause.getMessage());
		}
		assertNull(ref.value);
	}

	@org.junit.Test
	public void testConstructorWithThrowAndRef() throws NoSuchMethodException, IllegalAccessException, InstantiationException {
		Class<TestIterate> testIterateCls = ClassBuilder.builder(TestIterate.class)
				.withConstructor(List.of(Ref.class), sequence(
						set(property(arg(0), "value"), value(100)),
						throwException(RuntimeException.class, "test"))
				)
				.withMethod("iterate", throwException(new AssertionError()))
				.build()
				.defineClass(CLASS_LOADER);

		Ref ref = new Ref();
		try {
			testIterateCls.getConstructor(Ref.class).newInstance(ref);
		} catch (InvocationTargetException e) {
			Throwable cause = e.getCause();
			assertSame(RuntimeException.class, cause.getClass());
			assertEquals("test", cause.getMessage());
		}
		assertEquals(100, ref.value);
	}

	public static class Ref {
		public Object value;
	}

	public interface TestSeq {
		void seq(Ref ref);
	}

	public interface TestIterate {
		void iterate(Ref ref);
	}
}
