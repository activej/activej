package io.activej.specializer;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.IntUnaryOperator;

public class SpecializerTest {
	public static final class IntUnaryOperatorConst implements IntUnaryOperator {
		private final int value;

		public IntUnaryOperatorConst(int value) {this.value = value;}

		@Override
		public int applyAsInt(int operand) {
			return value;
		}
	}

	public static final class IntUnaryOperatorIdentity implements IntUnaryOperator {
		@Override
		public int applyAsInt(int operand) {
			return operand;
		}
	}

	public static final class IntUnaryOperatorSum implements IntUnaryOperator {
		private final IntUnaryOperator delegate1;
		private final IntUnaryOperator delegate2;

		public IntUnaryOperatorSum(IntUnaryOperator delegate1, IntUnaryOperator delegate2) {
			this.delegate1 = delegate1;
			this.delegate2 = delegate2;
		}

		@Override
		public int applyAsInt(int operand) {
			return delegate1.applyAsInt(operand) + delegate2.applyAsInt(operand);
		}
	}

	public static final class IntUnaryOperatorProduct implements IntUnaryOperator {
		private final IntUnaryOperator delegate1;
		private final IntUnaryOperator delegate2;

		public IntUnaryOperatorProduct(IntUnaryOperator delegate1, IntUnaryOperator delegate2) {
			this.delegate1 = delegate1;
			this.delegate2 = delegate2;
		}

		@Override
		public int applyAsInt(int operand) {
			return delegate1.applyAsInt(operand) * delegate2.applyAsInt(operand);
		}
	}

	@Test
	public void testIntUnaryOperator0() {
		TestClass0 instance = new TestClass0(new TestClass0(1));

		int expected = instance.apply(0);
		Specializer specializer = Specializer.create();
//				.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath());
		TestInterface specialized = specializer.specialize(instance);
		Assert.assertEquals(expected, specialized.apply(0));
	}

	@Test
	public void testIntUnaryOperator1() {
		IntUnaryOperator instance =
				new IntUnaryOperatorProduct(
						new IntUnaryOperatorSum(
								new IntUnaryOperatorSum(
										new IntUnaryOperatorIdentity(),
										new IntUnaryOperatorConst(5)),
								new IntUnaryOperatorConst(-5)),
						new IntUnaryOperatorConst(-1));
		int expected = instance.applyAsInt(5);
		Specializer specializer = Specializer.create();
//				.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath());
		IntUnaryOperator specialized = specializer.specialize(instance);
		Assert.assertEquals(expected, specialized.applyAsInt(5));
	}

	@Test
	public void name() {
		TestClass1 specializableClass = new TestClass1(new TestClass2(), new TestClass1(null, null));
		Specializer specializer = Specializer.create();
//				.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath());
		TestInterface specialized = specializer.specialize(specializableClass);
		int expected = specializableClass.apply(55);
		Assert.assertEquals(expected, specialized.apply(55));
	}

	@Test
	public void testIfElse() {
		TestInterface ifElseTestClass = new IfElseTestClass(x -> x * 2);
		Specializer specializer = Specializer.create();
//				.withBytecodeSaveDir(Paths.get("tmp"));
		TestInterface specialized = specializer.specialize(ifElseTestClass);
		Assert.assertEquals(6, specialized.apply(3));
	}

	@Test
	public void testMultipleFrames() {
		TestInterface ifElseTestClass = new MultipleFramesTestClass();
		Specializer specializer = Specializer.create();
		TestInterface specialized = specializer.specialize(ifElseTestClass);
		Assert.assertEquals(0, specialized.apply(3));
	}
}
