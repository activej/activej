package io.activej.dataflow.dsl;

import io.activej.codegen.expression.Expression;
import io.activej.common.tuple.Tuple2;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.Datasets;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.dsl.LambdaParser.LambdaExpression;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import org.jparsec.*;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.dataflow.dsl.ExpressionDef.expression;
import static io.activej.dataflow.dsl.LambdaUtils.getReturnType;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.jparsec.Parsers.sequence;

public final class DslParser {
	public static final Parser<Void> IGNORED = Parsers.or(
			Scanners.SQL_LINE_COMMENT,
			Scanners.SQL_BLOCK_COMMENT,
			Scanners.WHITESPACES
	).skipMany();

	public static final Parser<String> STRING_LITERAL = Terminals.StringLiteral.PARSER;
	public static final Parser<Integer> INT_LITERAL = Terminals.DecimalLiteral.PARSER.map(Integer::parseInt);
	public static final Parser<Float> FLOAT_LITERAL = Terminals.DecimalLiteral.PARSER.map(Float::parseFloat);

	private final Parser<AST.Query> parser;
	private final Terminals terminals;
	private final Parser<AST.DatasetExpression> expressionParser;
	private final Parser<LambdaExpression> lambdaParser;

	public DslParser(Parser<AST.Query> parser, Terminals terminals, Parser<AST.DatasetExpression> expressionParser, Parser<LambdaExpression> lambdaParser) {
		this.parser = parser;
		this.terminals = terminals;
		this.expressionParser = expressionParser;
		this.lambdaParser = lambdaParser;
	}

	public AST.Query parse(String query) {
		return parser.parse(query);
	}

	public Parser<Token> getTokenParser(String name) {
		return terminals.token(name);
	}

	public Parser<AST.DatasetExpression> getExpressionParser() {
		return expressionParser;
	}

	public Parser<LambdaExpression> getLambdaParser() {
		return lambdaParser;
	}

	public static DslParser create(List<ExpressionDef> expressions) {
		Stream<String> builtinKeywords = Stream.of("USE", "WRITE", "INTO", "REPEAT", "TIMES", "END");

		Stream<String> customKeywords = expressions.stream()
				.flatMap(e -> e.getKeywords().stream());

		Stream<String> builtinOperators = Stream.of("=", ".", "(", ")");

		Stream<String> customOperators = LambdaParser.getOperatorTokens()
				.sorted(Comparator.comparing(String::length).reversed());

		Terminals dslTerminals =
				Terminals.operators(Stream.concat(customOperators, builtinOperators).collect(toSet()))
						.words(Scanners.IDENTIFIER)
						.caseInsensitiveKeywords(Stream.concat(builtinKeywords, customKeywords).collect(toSet()))
						.build();

		Parser<?> dslTokenizer =
				Parsers.or(
						Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER,
						Terminals.DecimalLiteral.TOKENIZER,
						dslTerminals.tokenizer());

		Parser.Reference<AST.DatasetExpression> expressionRef = Parser.newReference();
		Parser.Reference<AST.Statement> statementRef = Parser.newReference();
		Parser.Reference<LambdaExpression> lambdaRef = Parser.newReference();

		Parser<AST.Identifier> identifier = Terminals.identifier().map(AST.Identifier::new);

		Parser<AST.Query> grammar = statementRef.lazy().many().map(AST.Query::new);

		DslParser parser = new DslParser(grammar.from(dslTokenizer, IGNORED), dslTerminals, expressionRef.lazy(), lambdaRef.lazy());

		lambdaRef.set(LambdaParser.create(parser));

		Parser<?>[] expressionParsers = new Parser[expressions.size() + 1];
		for (int i = 0; i < expressions.size(); i++) {
			expressionParsers[i] = expressions.get(i).getParser(parser);
		}
		expressionParsers[expressions.size()] = identifier;

		Parser<AST.DatasetExpression> expression = Parsers.or(expressionParsers).cast();

		expressionRef.set(expression);

		Parser<AST.Use> use =
				sequence(dslTerminals.token("use"), STRING_LITERAL).map(AST.Use::new);

		Parser<AST.Assignment> assignment =
				sequence(identifier.followedBy(dslTerminals.token("=")), expression,
						AST.Assignment::new);

		Parser<AST.Write> write =
				sequence(
						sequence(dslTerminals.token("write"), expression),
						sequence(dslTerminals.token("into"), STRING_LITERAL),
						AST.Write::new
				);

		Parser<AST.Repeat> repeat =
				sequence(
						sequence(dslTerminals.token("repeat"), INT_LITERAL.followedBy(dslTerminals.token("times"))),
						statementRef.lazy().many().followedBy(dslTerminals.token("end")).map(AST.Query::new),
						AST.Repeat::new
				);

		Parser<AST.Statement> statement =
				Parsers.or(use, write, repeat, assignment);

		statementRef.set(statement);

		return parser;
	}

	@SuppressWarnings("unchecked")
	private static Tuple2<Function<Object, Object>, Class<Object>> getFunction(ExpressionContext context, Dataset<?> dataset, int index) {
		ExpressionContext keyGroup = context.getOptionalGroup(index);
		if (keyGroup == null) {
			return new Tuple2<>(Function.identity(), (Class<Object>) dataset.valueType());
		}
		Function<Object, Object> function = keyGroup.getMapper(0);
		return new Tuple2<>(function, getReturnType(function));
	}

	@SuppressWarnings({"RedundantCast", "unchecked", "rawtypes"})
	private static Comparator<Object> getComparator(ExpressionContext context, int index) {
		ExpressionContext comparingBy = context.getOptionalGroup(index);
		Comparator<Object> comparator = comparingBy != null ?
				comparingBy.generateInstance(0) :
				(Comparator) Comparator.naturalOrder();
		return context.getOptionalGroup(index + 1) != null ? comparator.reversed() : comparator;
	}

	public static List<ExpressionDef> defaultExpressions() {
		return asList(
				expression("DATASET %str TYPE %str", context ->
						Datasets.datasetOfId(context.getString(0), context.getClass(1))),

				expression("FILTER %expr WITH %lambda", context -> {
					Dataset<Object> dataset = context.evaluateExpr(0);
					return Datasets.filter(dataset, context.getPredicate(1, dataset.valueType()));
				}),

				expression("MAP %expr BY %lambda", context -> {
					Function<Object, Object> mapper = context.getMapper(1);
					return Datasets.map(context.evaluateExpr(0), mapper, getReturnType(mapper));
				}),

				expression("REPARTITION SORT %expr", context ->
						Datasets.repartitionSort((LocallySortedDataset<?, ?>) context.evaluateExpr(0))),

				expression("JOIN %expr AND %expr WITH %str RESULT %str [BY %str]", context -> {
					SortedDataset<Object, Object> left = (SortedDataset<Object, Object>) context.evaluateExpr(0);
					SortedDataset<Object, Object> right = (SortedDataset<Object, Object>) context.evaluateExpr(1);
					Joiner<Object, Object, Object, Object> joiner = context.generateInstance(2);
					Class<Object> resultType = context.getClass(3);

					ExpressionContext keyGroup = context.getOptionalGroup(4);
					Function<Object, Object> fn = keyGroup != null ?
							keyGroup.getMapper(0) :
							Function.identity();

					return Datasets.join(left, right, joiner, resultType, fn);
				}),

				expression("SORT REDUCE REPARTITION REDUCE %expr TYPE %str " +
						"WITH %str [BY %str] [COMPARING WITH %str] [REVERSED] ACCUMULATING BY %str EXTRACTING BY %str", context -> {

					Dataset<Object> dataset = context.evaluateExpr(0);
					Class<Object> outputType = context.getClass(1);
					ReducerToResult<Object, Object, Object, Object> reducer = context.generateInstance(2);
					Tuple2<Function<Object, Object>, Class<Object>> fn = getFunction(context, dataset, 3);
					Comparator<Object> keyComparator = getComparator(context, 4);

					Class<Object> accumulatorType = context.getClass(6);
					Function<Object, Object> accumulatorKeyFunction = context.getMapper(7);

					return Datasets.sortReduceRepartitionReduce(
							dataset, reducer, fn.getValue2(), fn.getValue1(),
							keyComparator, accumulatorType,
							accumulatorKeyFunction, outputType);
				}),

				expression("LOCAL SORT %expr [BY %str] [COMPARING WITH %str] [REVERSED]", context -> {
					Dataset<Object> dataset = context.evaluateExpr(0);
					Tuple2<Function<Object, Object>, Class<Object>> fn = getFunction(context, dataset, 1);
					Comparator<Object> comparator = getComparator(context, 2);
					return Datasets.localSort(dataset, fn.getValue2(), fn.getValue1(), comparator);
				}),

				expression("CAST SORT %expr [BY %str] [COMPARING WITH %str] [REVERSED]", context -> {
					Dataset<Object> dataset = context.evaluateExpr(0);
					Tuple2<Function<Object, Object>, Class<Object>> fn = getFunction(context, dataset, 1);
					Comparator<Object> comparator = getComparator(context, 2);
					return Datasets.castToSorted(dataset, fn.getValue2(), fn.getValue1(), comparator);
				})
		);
	}
}
