package io.activej.types.scanner;

import java.util.Map;

public class TestClass2<Y, X> extends TestClass1<@Annotation2 X> {
	public int n;
	@Annotation1
	public X x;
	@Annotation2
	public Y y;
	public Map<@Annotation1 X, Object> mapTV;
}
