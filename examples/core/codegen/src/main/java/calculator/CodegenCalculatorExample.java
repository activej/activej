package calculator;

import io.activej.codegen.ClassGenerator;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import org.jparsec.OperatorTable;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Terminals;
import org.jparsec.Terminals.DecimalLiteral;

import java.util.function.DoubleUnaryOperator;

import static org.jparsec.Scanners.*;

public final class CodegenCalculatorExample {
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	private static final Parser<Void> IGNORED = Parsers.or(WHITESPACES, JAVA_LINE_COMMENT, JAVA_BLOCK_COMMENT).skipMany();

	private static final Terminals DELIMITERS = Terminals.operators("+", "-", "*", "/", "%", "^", "(", ")", "x");
	private static final Parser<?> LEXER = DELIMITERS.tokenizer().cast().or(DecimalLiteral.TOKENIZER);

	private static final Parser<Expression> NUMBER = DecimalLiteral.PARSER.map(s -> Expressions.value(Double.parseDouble(s)));

	private static final Parser<Expression> UNKNOWN = DELIMITERS.token("x").retn(Expressions.arg(0));

	private static final Parser.Reference<Expression> EXPRESSION_REF = Parser.newReference();

	private static final Parser<Expression> PARENS = EXPRESSION_REF.lazy().between(DELIMITERS.token("("), DELIMITERS.token(")"));

	private static final Parser<Expression> ATOM =
		Parsers.or(
			PARENS,
			NUMBER,
			UNKNOWN
		);

	//[START REGION_1]
	private static final Parser<Expression> EXPRESSION = new OperatorTable<Expression>()
		.infixl(DELIMITERS.token("+").retn(Expressions::add), 10)
		.infixl(DELIMITERS.token("-").retn(Expressions::sub), 10)
		.infixl(DELIMITERS.token("*").retn(Expressions::mul), 20)
		.infixl(DELIMITERS.token("/").retn(Expressions::div), 20)
		.infixl(DELIMITERS.token("%").retn(Expressions::rem), 20)
		.prefix(DELIMITERS.token("-").retn(Expressions::neg), 30)
		.infixr(DELIMITERS.token("^").retn((left, right) -> Expressions.staticCall(Math.class, "pow", left, right)), 40)
		.build(ATOM);

	//[END REGION_1]
	static {
		EXPRESSION_REF.set(EXPRESSION);
	}

	public static final Parser<Expression> PARSER = EXPRESSION.from(LEXER, IGNORED);

	//[START REGION_2]
	public static Class<DoubleUnaryOperator> compile(String expression) {
		return ClassGenerator.builder(DoubleUnaryOperator.class)
			.withMethod("applyAsDouble", PARSER.parse(expression))
			.build()
			.generateClass(CLASS_LOADER);
	}
	//[END REGION_2]

	//[START REGION_3]
	public static void main(String[] args) throws Exception {
		double x = -1;

		// manual code, superfast
		System.out.println(((2.0 + 2.0 * 2.0) * -x) + 5.0 + 1024.0 / (100.0 + 58.0) * 50.0 * 37.0 - 100.0 + 2.0 * Math.pow(x, 2.0) % 3.0);

		DoubleUnaryOperator instance = compile("((2 + 2 * 2) * -x) + 5 + 1024 / (100 + 58) * 50 * 37 - 100 + 2 * x ^ 2 % 3").getDeclaredConstructor().newInstance();

		// generated instance evaluation, literally equivalent to manual code (with a method call around it), except it was dynamically generated
		System.out.println(instance.applyAsDouble(x));
	}
	//[END REGION_3]
}
