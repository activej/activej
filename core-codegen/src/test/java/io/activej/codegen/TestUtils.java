package io.activej.codegen;

import static org.junit.Assert.assertEquals;

public final class TestUtils {
	public static void assertStaticConstantsCleared() {
		assertEquals("Some static constants have not been cleaned up", 0, ClassGenerator.getStaticConstantsSize());
	}
}
