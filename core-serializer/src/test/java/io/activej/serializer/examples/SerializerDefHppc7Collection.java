package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.AbstractSerializerDefCollection;
import io.activej.serializer.impl.SerializerDefNullable;

import java.util.function.Function;

import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerDefHppc7Collection extends AbstractSerializerDefCollection {
	// region creators
	public SerializerDefHppc7Collection(SerializerDef valueSerializer, Class<?> collectionType, Class<?> elementType, Class<?> collectionImplType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	public SerializerDefHppc7Collection(Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType, SerializerDef valueSerializer) {
		this(valueSerializer, collectionType, valueType, collectionImplType, false);
	}
	// endregion

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (nullable && compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			SerializerDefHppc7Collection serializer = new SerializerDefHppc7Collection(valueSerializer, encodeType, elementType, decodeType, false);
			return SerializerDefNullable.encode(serializer, staticEncoders, buf, pos, value, version, compatibilityLevel);
		}
		return super.encoder(staticEncoders, buf, pos, value, version, compatibilityLevel);
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		if (nullable && compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			SerializerDefHppc7Collection serializer = new SerializerDefHppc7Collection(valueSerializer, encodeType, elementType, decodeType, false);
			return SerializerDefNullable.decode(serializer, staticDecoders, in, version, compatibilityLevel);
		}
		return super.decoder(staticDecoders, in, version, compatibilityLevel);
	}

	@Override
	protected Expression collectionForEach(Expression collection, Class<?> valueType, Function<Expression, Expression> value) {
		try {
			String prefix = capitalize(elementType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return new ForEachHppcCollection(collection, iteratorType, value);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("There is no hppc cursor for " + elementType.getSimpleName(), e);
		}
	}

	@Override
	public SerializerDef ensureNullable() {
		return new SerializerDefHppc7Collection(valueSerializer, encodeType, elementType, decodeType, true);
	}
}
