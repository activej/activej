package calculator;

import io.activej.specializer.Specializer;
import org.jparsec.OperatorTable;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Terminals;
import org.jparsec.Terminals.DecimalLiteral;

import static org.jparsec.Scanners.*;

public final class SpecializerCalculatorExample {

	private static final Parser<Void> IGNORED = Parsers.or(WHITESPACES, JAVA_LINE_COMMENT, JAVA_BLOCK_COMMENT).skipMany();

	private static final Terminals DELIMITERS = Terminals.operators("+", "-", "*", "/", "%", "^", "(", ")", "x");
	private static final Parser<?> LEXER = DELIMITERS.tokenizer().cast().or(DecimalLiteral.TOKENIZER);

	private static final Parser<CalculatorExpression> NUMBER = DecimalLiteral.PARSER.map(s -> new Constant(Double.parseDouble(s)));

	private static final Parser<CalculatorExpression> UNKNOWN = DELIMITERS.token("x").retn(Unknown.INSTANCE);

	private static final Parser.Reference<CalculatorExpression> EXPRESSION_REF = Parser.newReference();

	private static final Parser<CalculatorExpression> PARENS = EXPRESSION_REF.lazy().between(DELIMITERS.token("("), DELIMITERS.token(")"));

	private static final Parser<CalculatorExpression> ATOM =
		Parsers.or(
			PARENS,
			NUMBER,
			UNKNOWN
		);

	//[START REGION_1]
	private static final Parser<CalculatorExpression> EXPRESSION = new OperatorTable<CalculatorExpression>()
		.infixl(DELIMITERS.token("+").retn(Sum::new), 10)
		.infixl(DELIMITERS.token("-").retn(Sub::new), 10)
		.infixl(DELIMITERS.token("*").retn(Mul::new), 20)
		.infixl(DELIMITERS.token("/").retn(Div::new), 20)
		.infixl(DELIMITERS.token("%").retn(Mod::new), 20)
		.prefix(DELIMITERS.token("-").retn(Neg::new), 30)
		.infixr(DELIMITERS.token("^").retn(Pow::new), 40)
		.build(ATOM);
	//[END REGION_1]

	static {
		EXPRESSION_REF.set(EXPRESSION);
	}

	public static final Parser<CalculatorExpression> PARSER = EXPRESSION.from(LEXER, IGNORED);

	private static final Specializer SPECIALIZER = Specializer.create(Thread.currentThread().getContextClassLoader());

	//[START REGION_2]
	public static void main(String[] args) {
		double x = -1;

		// manual code, superfast
		System.out.println(((2.0 + 2.0 * 2.0) * -x) + 5.0 + 1024.0 / (100.0 + 58.0) * 50.0 * 37.0 - 100.0 + 2.0 * Math.pow(x, 2.0) % 3.0);

		CalculatorExpression expression = PARSER.parse("((2 + 2 * 2) * -x) + 5 + 1024 / (100 + 58) * 50 * 37 - 100 + 2 * x ^ 2 % 3");

		System.out.println(expression);

		// tree-walking evaluation, super slow
		System.out.println(expression.evaluate(x));

		// specialized instance evaluation, about as fast as manual code
		CalculatorExpression specialized = SPECIALIZER.specialize(expression);
		System.out.println(specialized.evaluate(x));
	}
	//[END REGION_2]

	public interface CalculatorExpression {
		double evaluate(double x);
	}

	public static final class Constant implements CalculatorExpression {
		private final double value;

		public Constant(double value) {
			this.value = value;
		}

		@Override
		public double evaluate(double x) {
			return value;
		}

		@Override
		public String toString() {
			return "Constant(" + value + ")";
		}
	}

	public static final class Unknown implements CalculatorExpression {
		public static final Unknown INSTANCE = new Unknown();

		@Override
		public double evaluate(double x) {
			return x;
		}

		@Override
		public String toString() {
			return "Unknown";
		}
	}

	public static final class Neg implements CalculatorExpression {
		private final CalculatorExpression expression;

		public Neg(CalculatorExpression expression) {
			this.expression = expression;
		}

		@Override
		public double evaluate(double x) {
			return -expression.evaluate(x);
		}

		@Override
		public String toString() {
			return "Neg(" + expression + ")";
		}
	}

	public static final class Sum implements CalculatorExpression {
		private final CalculatorExpression left;
		private final CalculatorExpression right;

		public Sum(CalculatorExpression left, CalculatorExpression right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public double evaluate(double x) {
			return left.evaluate(x) + right.evaluate(x);
		}

		@Override
		public String toString() {
			return "Sum(" + left + ", " + right + ")";
		}
	}

	public static final class Sub implements CalculatorExpression {
		private final CalculatorExpression left;
		private final CalculatorExpression right;

		public Sub(CalculatorExpression left, CalculatorExpression right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public double evaluate(double x) {
			return left.evaluate(x) - right.evaluate(x);
		}

		@Override
		public String toString() {
			return "Sub(" + left + ", " + right + ")";
		}
	}

	public static final class Mul implements CalculatorExpression {
		private final CalculatorExpression left;
		private final CalculatorExpression right;

		public Mul(CalculatorExpression left, CalculatorExpression right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public double evaluate(double x) {
			return left.evaluate(x) * right.evaluate(x);
		}

		@Override
		public String toString() {
			return "Mul(" + left + ", " + right + ")";
		}
	}

	public static final class Div implements CalculatorExpression {
		private final CalculatorExpression left;
		private final CalculatorExpression right;

		public Div(CalculatorExpression left, CalculatorExpression right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public double evaluate(double x) {
			return left.evaluate(x) / right.evaluate(x);
		}

		@Override
		public String toString() {
			return "Div(" + left + ", " + right + ")";
		}
	}

	public static final class Mod implements CalculatorExpression {
		private final CalculatorExpression left;
		private final CalculatorExpression right;

		public Mod(CalculatorExpression left, CalculatorExpression right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public double evaluate(double x) {
			return left.evaluate(x) % right.evaluate(x);
		}

		@Override
		public String toString() {
			return "Mod(" + left + ", " + right + ")";
		}
	}

	public static final class Pow implements CalculatorExpression {
		private final CalculatorExpression left;
		private final CalculatorExpression right;

		public Pow(CalculatorExpression left, CalculatorExpression right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public double evaluate(double x) {
			return Math.pow(left.evaluate(x), right.evaluate(x));
		}

		@Override
		public String toString() {
			return "Pow(" + left + ", " + right + ")";
		}
	}
}
