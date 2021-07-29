package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeStringFormat;
import io.activej.serializer.annotations.SerializeSubclasses;
import io.activej.serializer.annotations.SerializeVarLength;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.activej.serializer.CompatibilityLevel.LEVEL_3_LE;
import static io.activej.serializer.StringFormat.*;

@SuppressWarnings("ALL")
@State(Scope.Benchmark)
public class SerializerBenchmark {
	private static final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
	private static final BinarySerializer<TestData> serializer = SerializerBuilder.create(definingClassLoader)
			.withCompatibilityLevel(LEVEL_3_LE)
			// .withGeneratedBytecodePath(Paths.get("tmp").toAbsolutePath())
			.build(TestData.class);
	private static final byte[] array = new byte[10000];

	public static class TestData {
		public enum TestEnum {
			ONE(1), TWO(2), THREE(3);

			TestEnum(@SuppressWarnings("UnusedParameters") int id) {
			}
		}

		@Serialize
		public boolean z;
		@Serialize
		public boolean z1;
		@Serialize
		public boolean z2;

		@Serialize
		public byte b;
		@Serialize
		public byte b1;
		@Serialize
		public byte b2;

		@Serialize
		public short s;
		@Serialize
		public short s1;
		@Serialize
		public short s2;

		@Serialize
		public char c;
		@Serialize
		public char c1;
		@Serialize
		public char c2;

		@Serialize
		public int i;
		@Serialize
		public int i1;
		@Serialize
		public int i2;

		@Serialize
		@SerializeVarLength
		public int vi = 0;
		@Serialize
		@SerializeVarLength
		public int vi1 = 0;
		@Serialize
		@SerializeVarLength
		public int vi2 = 0;

		@Serialize
		public long l;
		@Serialize
		public long l1;
		@Serialize
		public long l2;

		@Serialize
		@SerializeVarLength
		public long vl = 0;
		@Serialize
		@SerializeVarLength
		public long vl1 = 0;
		@Serialize
		@SerializeVarLength
		public long vl2 = 0;

		@Serialize
		public float f;
		@Serialize
		public float f1;
		@Serialize
		public float f2;

		@Serialize
		public double d;
		@Serialize
		public double d1;
		@Serialize
		public double d2;

		@Serialize
		@SerializeStringFormat(UTF8)
		public String string = "Hello, World!";
		@Serialize
		@SerializeStringFormat(ISO_8859_1)
		public String string1 = "Hello, World!";
		@Serialize
		@SerializeStringFormat(UTF16)
		public String string2 = "Hello, World!";
		@Serialize
		@SerializeStringFormat(UTF8_MB3)
		public String string3 = "Hello, World!";

		@Serialize
		public TestEnum en = TestEnum.ONE;
		@Serialize
		public TestEnum en1 = TestEnum.TWO;
		@Serialize
		public TestEnum en2 = TestEnum.THREE;

		@Serialize
		public List<TestDataElement> elements = new ArrayList<>();
	}

	public static class TestDataElement {
		@Serialize
		public boolean i;
		@Serialize
		public Map<Integer, TestDataSubElement> subelements = new HashMap<>();
	}

	@SerializeSubclasses({TestDataSubElementA.class, TestDataSubElementB.class})
	public abstract static class TestDataSubElement {
	}

	public static class TestDataSubElementA extends TestDataSubElement {
		@Serialize
		public int x;
	}

	public static class TestDataSubElementB extends TestDataSubElement {
		@Serialize
		public long y;
	}

	TestData testData1 = new TestData();
	TestData testData2;

	@Setup
	public void setup() {
		for (int i = 0; i < 10; i++) {
			TestDataElement e = new TestDataElement();
			for (int j = 0; j < 10; j++) {
				e.subelements.put(j * 2, new TestDataSubElementA());
				e.subelements.put(j * 2 + 1, new TestDataSubElementB());
			}
			testData1.elements.add(e);
		}
		serializer.encode(array, 0, testData1);
	}

	@Benchmark
	public void measureSerialization(Blackhole blackhole) {
		blackhole.consume(serializer.encode(array, 0, testData1));
		blackhole.consume(serializer.decode(array, 0));
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SerializerBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(5)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}
