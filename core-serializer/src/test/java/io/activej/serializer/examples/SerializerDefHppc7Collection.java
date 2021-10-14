package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.common.exception.UncheckedException;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.AbstractSerializerDefCollection;
import org.jetbrains.annotations.NotNull;

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerDefHppc7Collection extends AbstractSerializerDefCollection {
	public SerializerDefHppc7Collection(SerializerDef valueSerializer, Class<?> collectionType, Class<?> elementType, Class<?> collectionImplType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	public SerializerDefHppc7Collection(Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType, SerializerDef valueSerializer) {
		this(valueSerializer, collectionType, valueType, collectionImplType, false);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefHppc7Collection(valueSerializer, encodeType, elementType, decodeType, true);
	}

	@Override
	protected Expression doIterate(Expression collection, UnaryOperator<Expression> action) {
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

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, length);
	}

	@Override
	protected @NotNull Expression addToBuilder(Expression builder, Expression index, Expression element) {
		return call(builder, "add", element);
	}
}
