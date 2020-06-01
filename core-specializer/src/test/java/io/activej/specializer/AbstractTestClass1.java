package io.activej.specializer;

public abstract class AbstractTestClass1 implements TestInterface {
	private final int finalX;
//	private final TestInterface delegate;
//	private final AbstractTestClass1 testClass1;
//	private final TestClass2 testClass2;

	public AbstractTestClass1(TestInterface delegate, AbstractTestClass1 class1) {
//		this.testClass1 = class1;
		this.finalX = 1;
//		this.delegate = delegate;
	}

	@Override
	public int apply(int arg) {
		return finalX;
//		int y = 10;
//		return arg + y + finalX + //test() + apply(100) +
////				delegate.apply(arg) +
//				testClass1.finalX;
	}
}
