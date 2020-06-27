package io.activej.dataflow.dsl;

import io.activej.codegen.expression.Expression;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dsl.LambdaParser.LambdaExpression;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public final class ExpressionContext {
	private final EvaluationContext evaluationContext;
	private final List<Object> items;

	public ExpressionContext(EvaluationContext evaluationContext, List<Object> items) {
		this.evaluationContext = evaluationContext;
		this.items = items;
	}

	public EvaluationContext getEvaluationContext() {
		return evaluationContext;
	}

	private <T> T getItem(int position, Class<T> cls, String name) {
		Object object = items.get(position);
		if (!cls.isInstance(object)) {
			throw new IllegalStateException("Tried to get " + name + " at index " + position + ", got " + object);
		}
		return cls.cast(object);
	}

	public int getInt(int position) {
		return getItem(position, int.class, "an int");
	}

	public float getFloat(int position) {
		return getItem(position, float.class, "a float");
	}

	public String getString(int position) {
		return getItem(position, String.class, "a string");
	}

	public Class<Object> getClass(int position) {
		return evaluationContext.resolveClass(getString(position));
	}

	public <T, R> Function<T, R> getMapper(int position) {
		Object object = items.get(position);
		if (object instanceof String) {
			return evaluationContext.generateInstance((String) object);
		}
		return LambdaUtils.generateMapper((LambdaExpression) object);
	}

	public <T> Predicate<T> getPredicate(int position, Class<T> argumentType) {
		Object object = items.get(position);
		if (object instanceof String) {
			return evaluationContext.generateInstance((String) object);
		}
		return LambdaUtils.generatePredicate((LambdaExpression) object, argumentType);
	}

	public <T> T generateInstance(int position) {
		return evaluationContext.generateInstance(getString(position));
	}

	public <T> Dataset<T> evaluateExpr(int position) {
		return getItem(position, AST.DatasetExpression.class, "an expression").evaluate(evaluationContext);
	}

	@Nullable
	@SuppressWarnings("unchecked")
	public ExpressionContext getOptionalGroup(int position) {
		Object object = items.get(position);
		if (object == ExpressionDef.NO_GROUP) {
			return null;
		}
		if (!(object instanceof List)) {
			throw new IllegalStateException("Tried to get an optional group at index " + position + ", got " + object);
		}
		return new ExpressionContext(evaluationContext, (List<Object>) object);
	}
}
