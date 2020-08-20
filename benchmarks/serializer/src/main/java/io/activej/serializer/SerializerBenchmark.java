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

		@Serialize(order = 10)
		public boolean z;
		@Serialize(order = 11)
		public boolean z1;
		@Serialize(order = 12)
		public boolean z2;

		@Serialize(order = 20)
		public byte b;
		@Serialize(order = 21)
		public byte b1;
		@Serialize(order = 22)
		public byte b2;

		@Serialize(order = 30)
		public short s;
		@Serialize(order = 31)
		public short s1;
		@Serialize(order = 32)
		public short s2;

		@Serialize(order = 40)
		public char c;
		@Serialize(order = 41)
		public char c1;
		@Serialize(order = 42)
		public char c2;

		@Serialize(order = 50)
		public int i;
		@Serialize(order = 51)
		public int i1;
		@Serialize(order = 52)
		public int i2;

		@Serialize(order = 60)
		@SerializeVarLength
		public int vi = 0;
		@Serialize(order = 61)
		@SerializeVarLength
		public int vi1 = 0;
		@Serialize(order = 62)
		@SerializeVarLength
		public int vi2 = 0;

		@Serialize(order = 70)
		public long l;
		@Serialize(order = 71)
		public long l1;
		@Serialize(order = 72)
		public long l2;

		@Serialize(order = 80)
		@SerializeVarLength
		public long vl = 0;
		@Serialize(order = 81)
		@SerializeVarLength
		public long vl1 = 0;
		@Serialize(order = 82)
		@SerializeVarLength
		public long vl2 = 0;

		@Serialize(order = 90)
		public float f;
		@Serialize(order = 91)
		public float f1;
		@Serialize(order = 92)
		public float f2;

		@Serialize(order = 100)
		public double d;
		@Serialize(order = 101)
		public double d1;
		@Serialize(order = 102)
		public double d2;

		@Serialize(order = 110)
		@SerializeStringFormat(UTF8)
		public String string = "Hello, World!";
		@Serialize(order = 111)
		@SerializeStringFormat(ISO_8859_1)
		public String string1 = "Hello, World!";
		@Serialize(order = 112)
		@SerializeStringFormat(UTF16)
		public String string2 = "Hello, World!";
		@Serialize(order = 113)
		@SerializeStringFormat(UTF8_MB3)
		public String string3 = "Hello, World!";

		@Serialize(order = 120)
		public TestEnum en = TestEnum.ONE;
		@Serialize(order = 121)
		public TestEnum en1 = TestEnum.TWO;
		@Serialize(order = 122)
		public TestEnum en2 = TestEnum.THREE;

		@Serialize(order = 200)
		public List<TestDataElement> elements = new ArrayList<>();
	}

	public static class TestDataElement {
		@Serialize(order = 1)
		public boolean i;
		@Serialize(order = 2)
		public Map<Integer, TestDataSubElement> subelements = new HashMap<>();
	}

	@SerializeSubclasses({TestDataSubElementA.class, TestDataSubElementB.class})
	public abstract static class TestDataSubElement {
	}

	public static class TestDataSubElementA extends TestDataSubElement {
		@Serialize(order = 1)
		public int x;
	}

	public static class TestDataSubElementB extends TestDataSubElement {
		@Serialize(order = 1)
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
