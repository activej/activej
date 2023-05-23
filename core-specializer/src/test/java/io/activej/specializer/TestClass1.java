package io.activej.specializer;

import java.io.IOException;

@SuppressWarnings("all")
public class TestClass1 extends AbstractTestClass1 {
	public final int finalX;
	public final TestInterface delegate;
	public final TestClass1 testClass1;

	public Integer mutableField = 123;

	public TestClass1(TestInterface delegate, TestClass1 class1) {
		super(delegate, class1);
		this.finalX = 2;
		this.testClass1 = class1;
		this.delegate = delegate;
	}

	public static int testStatic() {
		return 111;
	}

	public int test() {
		return super.apply(2) + mutableField;
	}

	public Integer getMutableField() {
		return mutableField;
	}

	public void setMutableField(Integer mutableField) {
		this.mutableField = mutableField;
	}

	TestInterface self() {
		return this;
	}

	@Override
	public int apply(int arg) {
		String str = new String();
		((Object) str).toString().length();
		try {
			synchronized (this) {
				int sum = 0;
				for (int i = 0; i < 10; i++) {
					sum += i;
					throw new IOException();
				}
			}
		} catch (IOException e) {
			int sum = 0;
			for (int i = 1; i < 10; i++) {
				sum += i;
			}
			return sum;
		} catch (Throwable e) {
			int sum = 0;
			for (int i = 2; i < 10; i++) {
				sum += i;
			}
			return sum;
		}

		synchronized (this) {
			setMutableField(getMutableField() + 1);
			int y = 10;
			return y + finalX + super.apply(100) +
				delegate.apply(arg) +
				testClass1.finalX + testStatic() + test() + mutableField;
		}
	}
}
