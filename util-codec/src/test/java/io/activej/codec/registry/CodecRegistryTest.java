package io.activej.codec.registry;

import io.activej.codec.StructuredCodec;
import io.activej.common.reflection.TypeT;
import io.activej.common.tuple.Tuple1;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public final class CodecRegistryTest {
	private static final CodecRegistry REGISTRY = CodecRegistry.createDefault();

	@Test
	public void enumTest() {
		REGISTRY.get(MyEnum.class);
	}

	@Test
	public void genericEnumTest() {
		StructuredCodec<Tuple1<MyEnum>> codec = REGISTRY.get(new TypeT<Tuple1<MyEnum>>() {});
		assertNotNull(codec);
	}

	private enum MyEnum {
		A, B, C
	}
}
