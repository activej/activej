package io.activej.serializer.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;

import java.util.Map;
import java.util.Set;

public abstract class ForwardingSerializerDef implements SerializerDef {
	protected final SerializerDef serializer;

	public ForwardingSerializerDef(SerializerDef serializer) {this.serializer = serializer;}

	@Override
	public void accept(Visitor visitor) {
		serializer.accept(visitor);
	}

	@Override
	public Set<Integer> getVersions() {
		return serializer.getVersions();
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return serializer.isInline(version, compatibilityLevel);
	}

	@Override
	public Class<?> getEncodeType() {
		return serializer.getEncodeType();
	}

	@Override
	public Class<?> getDecodeType() {
		return serializer.getDecodeType();
	}

	@Override
	public Map<Object, Expression> getEncoderInitializer() {
		return serializer.getEncoderInitializer();
	}

	@Override
	public Map<Object, Expression> getDecoderInitializer() {
		return serializer.getDecoderInitializer();
	}

	@Override
	public Map<Object, Expression> getEncoderFinalizer() {
		return serializer.getEncoderFinalizer();
	}

	@Override
	public Map<Object, Expression> getDecoderFinalizer() {
		return serializer.getDecoderFinalizer();
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return serializer.encoder(staticEncoders, buf, pos, value, version, compatibilityLevel);
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return serializer.decoder(staticDecoders, in, version, compatibilityLevel);
	}
}
