package io.activej.serializer.def;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.SerializerDef;

import java.util.Set;

public abstract class ForwardingSerializerDef implements SerializerDef {
	protected abstract SerializerDef serializer();

	@Override
	public void accept(Visitor visitor) {
		serializer().accept(visitor);
	}

	@Override
	public Set<Integer> getVersions() {
		return serializer().getVersions();
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return serializer().isInline(version, compatibilityLevel);
	}

	@Override
	public Class<?> getEncodeType() {
		return serializer().getEncodeType();
	}

	@Override
	public Class<?> getDecodeType() {
		return serializer().getDecodeType();
	}

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return serializer().encode(staticEncoders, buf, pos, value, version, compatibilityLevel);
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return serializer().decode(staticDecoders, in, version, compatibilityLevel);
	}
}
