package io.activej.serializer;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.util.Utils.get;

//@State(Scope.Benchmark)
public class TableSwitchBenchmark {

	static final Object[] keys = new Object[1000];

	static {
		for (int i = 0; i < keys.length; i++) {
			keys[i] = new Object();
		}
	}

	public interface Test {
		String test(int i);
	}

	public interface TestS {
		String test(Object k);
	}

	private static final Test test1 = ClassBuilder.create(Test.class)
			.withMethod("test",
					get(() -> {
						Expression result = value("---");
						for (int i = 0; i < keys.length; i++) {
							result = ifRefNe(arg(0), value(i), result, value("X" + i));
						}
						return result;
					}))
			.defineClassAndCreateInstance(DefiningClassLoader.create());

	private static final Test test2 = ClassBuilder.create(Test.class)
			.withMethod("test", String.class, List.of(int.class),
					get(() -> {
						Map<Integer, Expression> cases = new HashMap<>();
						for (int i = 0; i < keys.length; i++) {
							cases.put(i, value("X" + i));
						}

						return Expressions.tableSwitch(arg(0), cases, value("---"));
					}))
			.defineClassAndCreateInstance(DefiningClassLoader.create());

	private static final TestS testS1 = ClassBuilder.create(TestS.class)
			.withMethod("test",
					let(arg(0), arg -> {
						Expression result = value("---");
						for (int i = keys.length - 1; i >= 0; i--) {
							result = ifRefNe(arg, value(keys[i]), result, value("X" + i));
						}
						return result;
					}))
			.defineClassAndCreateInstance(DefiningClassLoader.create());

	private static final TestS testS2 = ClassBuilder.create(TestS.class)
			.withMethod("test",
					let(staticCall(System.class, "identityHashCode", arg(0)), key -> {
						Map<Integer, Expression> cases = new HashMap<>();
						for (int i = 0; i < keys.length; i++) {
							cases.put(System.identityHashCode(keys[i]), value("X" + i));
						}
						return Expressions.tableSwitch(key, cases, value("---"));
					}))
			.defineClassAndCreateInstance(DefiningClassLoader.create());

	@Benchmark
	public void measureTableSwitch1(Blackhole blackhole) {
		for (int i = 0; i < keys.length; i++) {
			blackhole.consume(test1.test(i));
		}
	}

	@Benchmark
	public void measureTableSwitch2(Blackhole blackhole) {
		for (int i = 0; i < keys.length; i++) {
			blackhole.consume(test2.test(i));
		}
	}

	@Benchmark
	public void measureTableSwitchS1(Blackhole blackhole) {
		for (int i = 0; i < keys.length; i++) {
			blackhole.consume(testS1.test(keys[i]));
		}
	}

	@Benchmark
	public void measureTableSwitchS2(Blackhole blackhole) {
		for (int i = 0; i < keys.length; i++) {
			blackhole.consume(testS2.test(keys[i]));
		}
	}

	public static void main(String[] args) throws RunnerException {
		System.out.println(test1.test(0));
		System.out.println(test2.test(0));
		System.out.println(testS1.test(keys[0]));
		System.out.println(testS2.test(keys[0]));

		Options opt = new OptionsBuilder()
				.forks(2)
				.warmupIterations(2)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(3)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}

}
