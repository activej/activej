package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;

import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.serializer.util.Utils.hashInitialSize;

public final class SerializerDef_Hppc7HashSet extends SerializerDef_Hppc7RegularCollection {
	public SerializerDef_Hppc7HashSet(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		this(valueSerializer, collectionType, collectionImplType, valueType, false);
	}

	private SerializerDef_Hppc7HashSet(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> elementType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDef_Hppc7HashSet(valueSerializer, encodeType, decodeType, elementType, true);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, hashInitialSize(length));
	}
}
