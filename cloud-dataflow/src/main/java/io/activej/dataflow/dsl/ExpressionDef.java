package io.activej.dataflow.dsl;

import io.activej.dataflow.dataset.Dataset;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class ExpressionDef {

	private static final Parser<Void> WHITESPACE = Scanners.WHITESPACES.skipMany();

	private static final Parser<Keywords> KEYWORDS =
			Scanners.IDENTIFIER.sepBy(WHITESPACE).map(Keywords::new);

	private static final Parser<Placeholder> PLACEHOLDER =
			Scanners.isChar('%').next(Scanners.IDENTIFIER)
					.map(s -> new Placeholder(PlaceholderType.valueOf(s.toUpperCase())));

	private static final Parser.Reference<List<ExpressionToken>> TOKENS_REF = Parser.newReference();

	private static final Parser<OptionalGroup> OPTIONAL_GROUP = TOKENS_REF.lazy()
			.between(Scanners.isChar('['), Scanners.isChar(']'))
			.map(OptionalGroup::new);

	private static final Parser<ExpressionToken> TOKEN = Parsers.or(PLACEHOLDER, OPTIONAL_GROUP, KEYWORDS);

	private static final Parser<List<ExpressionToken>> TOKENS =
			TOKEN.sepBy(WHITESPACE).between(WHITESPACE, WHITESPACE);

	static {
		TOKENS_REF.set(TOKENS);
	}

	static final Object NO_GROUP = new Object() {
		@Override
		public String toString() {
			return "<NO_GROUP>";
		}
	};

	private final List<ExpressionToken> tokens;
	private final Function<ExpressionContext, Dataset<?>> evaluationCallback;

	private ExpressionDef(List<ExpressionToken> tokens, Function<ExpressionContext, Dataset<?>> evaluationCallback) {
		this.tokens = tokens;
		this.evaluationCallback = evaluationCallback;
	}

	public static ExpressionDef expression(String syntax, Function<ExpressionContext, Dataset<?>> evaluationCallback) {
		return new ExpressionDef(TOKENS.parse(syntax), evaluationCallback);
	}

	private static void getKeywordsRecursive(List<String> result, List<ExpressionToken> tokens) {
		for (ExpressionToken token : tokens) {
			if (token instanceof Keywords) {
				result.addAll(((Keywords) token).value);
			} else if (token instanceof OptionalGroup) {
				getKeywordsRecursive(result, ((OptionalGroup) token).tokens);
			}
		}
	}

	public List<String> getKeywords() {
		List<String> keywords = new ArrayList<>();
		getKeywordsRecursive(keywords, tokens);
		return keywords;
	}

	private static Parser<List<Object>> constructParser(DslParser parser, List<ExpressionToken> tokens) {
		Parser<?>[] parsers = new Parser[tokens.size()];
		for (int i = 0; i < tokens.size(); i++) {
			parsers[i] = tokens.get(i).getParser(parser);
		}
		return Parsers.array(parsers)
				.map(result -> Arrays.stream(result).filter(Objects::nonNull).collect(toList()));
	}

	@SuppressWarnings("unchecked")
	private static void toStringRecursive(StringBuilder sb, List<ExpressionToken> tokens, List<Object> items) {
		int index = 0;
		for (int i = 0; i < tokens.size(); i++) {
			ExpressionToken token = tokens.get(i);
			if (token instanceof Keywords) {
				List<String> value = ((Keywords) token).value;
				for (int j = 0; j < value.size() - 1; j++) {
					sb.append(value.get(j)).append(' ');
				}
				sb.append(value.get(value.size() - 1));
			} else {
				Object param = items.get(index++);
				if (param instanceof List) {
					toStringRecursive(sb, ((OptionalGroup) token).tokens, (List<Object>) param);
				} else if (param == NO_GROUP) {
					continue; // to skip the extra space
				} else {
					sb.append(param);
				}
			}
			// really make sure no extra spaces are added at the end of the string
			if (i != tokens.size() - 1 && (index == items.size() - 1 || items.get(index) != NO_GROUP)) {
				sb.append(' ');
			}
		}
	}

	public Parser<AST.DatasetExpression> getParser(DslParser parser) {
		return constructParser(parser, tokens)
				.map(items -> new AST.DatasetExpression() {
					@Override
					@SuppressWarnings("unchecked")
					public <T> Dataset<T> evaluate(EvaluationContext context) {
						return (Dataset<T>) evaluationCallback.apply(new ExpressionContext(context, items));
					}

					@Override
					public String toString() {
						StringBuilder sb = new StringBuilder();
						toStringRecursive(sb, tokens, items);
						return sb.toString();
					}
				});
	}

	private interface ExpressionToken {

		Parser<?> getParser(DslParser parser);
	}

	private static final class Keywords implements ExpressionToken {
		public final List<String> value;

		public Keywords(List<String> value) {
			this.value = value;
		}

		@Override
		public Parser<?> getParser(DslParser parser) {
			Parser<?>[] parsers = new Parser[value.size()];
			for (int i = 0; i < value.size(); i++) {
				parsers[i] = parser.getTokenParser(value.get(i));
			}
			return Parsers.sequence(parsers).map($ -> null).atomic();
		}

		@Override
		public String toString() {
			return String.join(" ", value);
		}
	}

	private enum PlaceholderType {
		EXPR, STR, INT, FLOAT, LAMBDA
	}

	private static final class Placeholder implements ExpressionToken {
		public final PlaceholderType type;

		public Placeholder(PlaceholderType type) {
			this.type = type;
		}

		@Override
		public Parser<?> getParser(DslParser parser) {
			switch (type) {
				case INT:
					return DslParser.INT_LITERAL;
				case FLOAT:
					return DslParser.FLOAT_LITERAL;
				case STR:
					return DslParser.STRING_LITERAL;
				case EXPR:
					return parser.getExpressionParser();
				case LAMBDA:
					return DslParser.STRING_LITERAL.cast().or(parser.getLambdaParser());
			}
			throw new NullPointerException("Placeholder.type is null");
		}

		@Override
		public String toString() {
			return "%" + type.name().toLowerCase();
		}
	}

	private static final class OptionalGroup implements ExpressionToken {
		public final List<ExpressionToken> tokens;

		public OptionalGroup(List<ExpressionToken> tokens) {
			this.tokens = tokens;
		}

		@Override
		public Parser<?> getParser(DslParser parser) {
			return constructParser(parser, tokens).cast().optional(NO_GROUP);
		}

		@Override
		public String toString() {
			return tokens.stream()
					.map(ExpressionToken::toString)
					.collect(joining(" ", "[", "]"));
		}
	}
}
