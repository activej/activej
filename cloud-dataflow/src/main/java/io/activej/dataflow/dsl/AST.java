package io.activej.dataflow.dsl;

import io.activej.codegen.expression.Expression;
import io.activej.dataflow.dataset.Dataset;

import java.util.List;

import static io.activej.dataflow.dataset.Datasets.consumerOfId;
import static java.util.stream.Collectors.joining;

public final class AST {

	public interface Statement {

		void evaluate(EvaluationContext context);
	}

	public interface DatasetExpression {

		<T> Dataset<T> evaluate(EvaluationContext context);
	}

	public static class Query implements Statement {
		public final List<Statement> statements;

		public Query(List<Statement> statements) {
			this.statements = statements;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			statements.forEach(statement -> statement.evaluate(context));
		}

		@Override
		public String toString() {
			return statements.stream().map(Statement::toString).collect(joining("\n"));
		}
	}

	private static String stringLiteral(String value) {
		return "\"" +
				value.replace("\\", "\\\\")
						.replace("\"", "\\\"")
						.replace("\n", "\\n")
						.replace("\r", "\\r")
						.replace("\t", "\\t")
						.replace("\b", "\\b")
						.replace("\f", "\\f")
				+ "\"";
	}

	public static final class Identifier implements DatasetExpression {
		public final String name;

		public Identifier(String name) {
			this.name = name;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> Dataset<T> evaluate(EvaluationContext context) {
			return (Dataset<T>) context.get(name);
		}

		@Override
		public String toString() {
			return name;
		}
	}

	public static final class Use implements Statement {
		public final String prefix;

		public Use(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			context.addPrefix(prefix);
		}

		@Override
		public String toString() {
			return "USE " + stringLiteral(prefix);
		}
	}

	public static final class Assignment implements Statement {
		public final Identifier identifier;
		public final DatasetExpression expression;

		public Assignment(Identifier identifier, DatasetExpression expression) {
			this.identifier = identifier;
			this.expression = expression;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			context.put(identifier.name, expression.evaluate(context));
		}

		@Override
		public String toString() {
			return identifier + " = " + expression;
		}
	}

	public static final class Write implements Statement {
		public final DatasetExpression source;
		public final String destination;

		public Write(DatasetExpression source, String destination) {
			this.source = source;
			this.destination = destination;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			consumerOfId(source.evaluate(context), destination)
					.channels(context.getContext());
		}

		@Override
		public String toString() {
			return "WRITE " + source + " INTO " + stringLiteral(destination);
		}
	}

	public static final class Repeat implements Statement {
		public final int times;
		public final Query query;

		public Repeat(int times, Query query) {
			this.times = times;
			this.query = query;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			for (int i = 0; i < times; i++) {
				query.evaluate(context);
			}
		}

		@Override
		public String toString() {
			return "REPEAT " + times + " TIMES\n    " +
					query.statements.stream().map(Statement::toString).collect(joining("\n    ")) +
					"\nEND";
		}
	}
}
