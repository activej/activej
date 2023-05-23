package io.activej.specializer;

public class MultipleFramesTestClass implements TestInterface {
	@Override
	public int apply(int arg) {
		long k = -1;
		{
			int i = 42;
			int x = 43;
			int x1 = 43;
			int x2 = 43;
			System.out.println(k);
			System.out.println(i);
			System.out.println(x);
			System.out.println(x1);
			System.out.println(x2);
		}

		{
			long j = -1;
			long o = 454;
			System.out.println(k);
			System.out.println(j);
			System.out.println(o);
		}

		return 0;
	}
}
