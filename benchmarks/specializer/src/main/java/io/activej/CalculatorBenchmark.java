package io.activej;

import calculator.CodegenCalculatorExample;
import calculator.SpecializerCalculatorExample;
import calculator.SpecializerCalculatorExample.CalculatorExpression;
import io.activej.specializer.Specializer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleUnaryOperator;

@State(Scope.Benchmark)
public class CalculatorBenchmark {
	static final Specializer SPECIALIZER = Specializer.create();

	CalculatorExpression ast;
	CalculatorExpression manual;
	DoubleUnaryOperator generated;
	CalculatorExpression specialized;

	@Setup
	public void setup() throws Exception {
		String source = "((2 + 2 * 2) * -x) + 5 + 1024 / (100 + 58) * 50 * 37 - 100 + 2 * x ^ 2 % 3";

		ast = SpecializerCalculatorExample.PARSER.parse(source);
		manual = x -> ((2.0 + 2.0 * 2.0) * -x) + 5.0 + 1024.0 / (100.0 + 58.0) * 50.0 * 37.0 - 100.0 + 2.0 * (Math.pow(x, 2.0)) % 3.0;
		generated = CodegenCalculatorExample.compile(source).getDeclaredConstructor().newInstance();
		specialized = SPECIALIZER.specialize(ast);
	}

	@Benchmark
	public void ast(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(ast.evaluate(i));
		}
	}

	@Benchmark
	public void manual(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(manual.evaluate(i));
		}
	}

	@Benchmark
	public void generated(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(generated.applyAsDouble(i));
		}
	}

	@Benchmark
	public void specialized(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(specialized.evaluate(i));
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
			.include(CalculatorBenchmark.class.getSimpleName())
			.forks(2)
			.warmupIterations(3)
			.warmupTime(TimeValue.seconds(1L))
			.measurementIterations(5)
			.measurementTime(TimeValue.seconds(2L))
			.mode(Mode.AverageTime)
			.timeUnit(TimeUnit.NANOSECONDS)
			.build();

		new Runner(opt).run();
	}
}
