package io.activej.dataflow.dsl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import org.jetbrains.annotations.Nullable;
import org.jparsec.*;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.dataflow.dsl.LambdaParser.Arity.BINARY;
import static io.activej.dataflow.dsl.LambdaParser.Arity.UNARY;

public final class LambdaParser {
	public static Parser<LambdaExpression> create(DslParser parser) {
		Parser.Reference<LambdaExpression> lambdaExprRef = Parser.newReference();

		Parser<Token> dot = parser.getTokenParser(".");

		Parser<LambdaExpression> parensParser = lambdaExprRef.lazy()
				.between(parser.getTokenParser("("), parser.getTokenParser(")"));

		Parser<LambdaExpression> simpleExpr = Parsers.or(
				parensParser,

				dot.next(Terminals.identifier())
						.many1()
						.map(strs ->
								(LambdaExpression) argType -> {
									Expression reference = cast(arg(0), argType);
									// java has no proper foldLeft (at least in jdk8)
									for (String str : strs) {
										reference = property(reference, str);
									}
									return reference;
								})
						.or(dot.retn(argType -> cast(arg(0), argType))),

				Parsers.or(DslParser.INT_LITERAL.map(Integer::longValue), DslParser.FLOAT_LITERAL, DslParser.STRING_LITERAL)
						.map(value -> $ -> value(value))
		);

		OperatorTable<LambdaExpression> operatorTable = new OperatorTable<>();
		for (OpType op : OpType.values()) {
			Parser<Token> tokenParser = parser.getTokenParser(op.token);
			if (op.arity == BINARY) {
				operatorTable.infixl(tokenParser.retn(op::compile), op.precedence);
			} else if (op.arity == UNARY) {
				operatorTable.prefix(tokenParser.retn(expr -> op.compile(expr, null)), op.precedence);
			}
		}
		lambdaExprRef.set(operatorTable.build(simpleExpr));
		return parensParser.cast();
	}

	public static Stream<String> getOperatorTokens() {
		return Arrays.stream(OpType.values()).map(t -> t.token);
	}

	public enum Arity {
		UNARY, BINARY
	}

	// precedence is the same as in Java
	public enum OpType {
		OR(BINARY, "||", 10, Expressions::or),
		AND(BINARY, "&&", 20, Expressions::and),
		EQ(BINARY, "==", 30, Expressions::cmpEq), NE(BINARY, "!=", 30, Expressions::cmpNe),
		LE(BINARY, "<=", 40, Expressions::cmpLe), GE(BINARY, ">=", 40, Expressions::cmpGe), LT(BINARY, "<", 40, Expressions::cmpLt), GT(BINARY, ">", 40, Expressions::cmpGt),
		ADD(BINARY, "+", 50, Expressions::add), SUB(BINARY, "-", 50, Expressions::sub),
		MUL(BINARY, "*", 60, Expressions::mul), DIV(BINARY, "/", 60, Expressions::div), MOD(BINARY, "%", 60, Expressions::rem),

		NOT(UNARY, "!", 70, (expression, $) -> Expressions.not(expression)),
		MINUS(UNARY, "-", 70, (expression, $) -> Expressions.neg(expression)),
		;

		public final Arity arity;
		public final String token;
		public final int precedence;
		private final BiFunction<Expression, Expression, Expression> codegen;

		OpType(Arity arity, String token, int precedence, BiFunction<Expression, Expression, Expression> codegen) {
			this.arity = arity;
			this.token = token;
			this.precedence = precedence;
			this.codegen = codegen;
		}

		/**
		 * Right expression can be null for unary operators
		 */
		public LambdaExpression compile(LambdaExpression left, @Nullable LambdaExpression right) {
			return argType -> codegen.apply(left.compile(argType), right != null ? right.compile(argType) : null);
		}
	}

	@FunctionalInterface
	public interface LambdaExpression {

		Expression compile(Class<?> argumentType);
	}
}
