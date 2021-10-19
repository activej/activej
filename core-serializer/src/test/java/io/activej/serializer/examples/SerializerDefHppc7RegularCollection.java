package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.common.exception.UncheckedException;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.SerializerDefRegularCollection;
import org.jetbrains.annotations.NotNull;

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;

public class SerializerDefHppc7RegularCollection extends SerializerDefRegularCollection {
	public SerializerDefHppc7RegularCollection(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		this(valueSerializer, collectionType, collectionImplType, valueType, false);
	}

	protected SerializerDefHppc7RegularCollection(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> elementType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	@Override
	protected @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefHppc7RegularCollection(valueSerializer, encodeType, decodeType, elementType, true);
	}

	@Override
	protected @NotNull Expression doIterate(Expression collection, UnaryOperator<Expression> action) {
		try {
			String prefix = capitalize(elementType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return iterateIterable(collection,
					it -> action.apply(
							property(cast(it, iteratorType), "value")));
		} catch (ClassNotFoundException e) {
			throw UncheckedException.of(e);
		}
	}
}
