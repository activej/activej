package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.serializer.util.Utils.hashInitialSize;

public final class SerializerDefHppc7HashSet extends SerializerDefHppc7RegularCollection {
	public SerializerDefHppc7HashSet(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		this(valueSerializer, collectionType, collectionImplType, valueType, false);
	}

	private SerializerDefHppc7HashSet(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> elementType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	@Override
	protected @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefHppc7HashSet(valueSerializer, encodeType, decodeType, elementType, true);
	}

	@Override
	protected @NotNull Expression createBuilder(Expression length) {
		return constructor(decodeType, hashInitialSize(length));
	}
}
