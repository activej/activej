package io.activej.types.scanner;

import java.util.List;

public class TestClass1<T> extends TestClass0 {
	public int n;
	public @Annotation2("abc") T t;
	public List<T> listT;
}
