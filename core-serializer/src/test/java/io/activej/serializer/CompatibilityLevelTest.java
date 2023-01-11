package io.activej.serializer;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeStringFormat;
import io.activej.serializer.impl.SerializerDef_ByteBuffer;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unused")
public class CompatibilityLevelTest {

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Test
	public void strings() {
		byte[] level1Bytes = doTestStrings(CompatibilityLevel.LEVEL_1);
		byte[] level2Bytes = doTestStrings(CompatibilityLevel.LEVEL_2);
		byte[] level3Bytes = doTestStrings(CompatibilityLevel.LEVEL_3);
		byte[] level4Bytes = doTestStrings(CompatibilityLevel.LEVEL_4);

		assertThat(level1Bytes, not(equalTo(level2Bytes)));
		assertThat(level2Bytes, allOf(equalTo(level3Bytes), equalTo(level4Bytes)));
	}

	@Test
	public void nullables() {
		byte[] level1Bytes = doTestNullables(CompatibilityLevel.LEVEL_1);
		byte[] level2Bytes = doTestNullables(CompatibilityLevel.LEVEL_2);
		byte[] level3Bytes = doTestNullables(CompatibilityLevel.LEVEL_3);
		byte[] level4Bytes = doTestNullables(CompatibilityLevel.LEVEL_4);

		assertThat(level1Bytes, equalTo(level2Bytes));
		assertThat(level2Bytes, not(equalTo(level3Bytes)));
		assertThat(level4Bytes, allOf(equalTo(level3Bytes), not(equalTo(level1Bytes))));
	}

	@Test
	public void booleans() {
		byte[] level1Bytes = doTestBooleans(CompatibilityLevel.LEVEL_1);
		byte[] level2Bytes = doTestBooleans(CompatibilityLevel.LEVEL_2);
		byte[] level3Bytes = doTestBooleans(CompatibilityLevel.LEVEL_3);
		byte[] level4Bytes = doTestBooleans(CompatibilityLevel.LEVEL_4);

		assertThat(level1Bytes, allOf(equalTo(level2Bytes), equalTo(level3Bytes), not(equalTo(level4Bytes))));
	}

	private static byte[] doTestStrings(CompatibilityLevel compatibilityLevel) {
		String string = "\u1234\u5678\u9012";
		TestStrings strings = new TestStrings();
		strings.stringIso = string;
		strings.stringUtf8 = string;
		strings.stringUtf8Mb3 = string;
		strings.stringUtf16 = string;

		return doTestPreload(TestStrings.class, strings, compatibilityLevel, "strings");
	}

	private static byte[] doTestNullables(CompatibilityLevel compatibilityLevel) {
		TestNullables nullables = new TestNullables();
		nullables.notNullArray = new String[]{"test1", "test2"};
		nullables.notNullByteBuffer = ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8));
		nullables.notNullCollection = List.of("test1", "test2");
		nullables.notNullEnum = TestEnum.ONE;
		nullables.notNullList = List.of("test1", "test2");
		nullables.notNullMap = new LinkedHashMap<>();
		nullables.notNullMap.put(1, "test1");
		nullables.notNullMap.put(2, "test2");

		nullables.notNullSet = new LinkedHashSet<>(List.of("test1", "test2"));
		nullables.notNullSubclass = 12345;

		return doTestPreload(TestNullables.class, nullables, compatibilityLevel, "nullables");
	}

	private static byte[] doTestBooleans(CompatibilityLevel compatibilityLevel) {
		TestBooleans booleans = new TestBooleans();
		booleans.falseBoolean = Boolean.FALSE;
		booleans.trueBoolean = Boolean.TRUE;

		return doTestPreload(TestBooleans.class, booleans, compatibilityLevel, "booleans");
	}

	private static <T> byte[] doTestPreload(Class<T> aClass, T item, CompatibilityLevel compatibilityLevel, String filePrefix) {
		BinarySerializer<T> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.with(ByteBuffer.class, $ -> new SerializerDef_ByteBuffer())
				.withCompatibilityLevel(compatibilityLevel)
				.build(aClass);

		byte[] arr = new byte[1000];
		int length = serializer.encode(arr, 0, item);
		T decoded = serializer.decode(arr, 0);

		String filename = filePrefix + "_level_" +
				compatibilityLevel.getLevel() + (compatibilityLevel.isLittleEndian() ? "LE" : "") +
				".dat";
		byte[] downloaded = download(filename);
		assertArrayEquals(Arrays.copyOf(arr, length), downloaded);
		T preload = serializer.decode(downloaded, 0);

		assertEquals(decoded, preload);
		return downloaded;
	}

	private static byte[] download(String filename) {
		try (InputStream stream = CompatibilityLevelTest.class.getResourceAsStream("/compatibility/" + filename)) {
			assert stream != null;
			return stream.readAllBytes();
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static class TestStrings {
		@Serialize
		@SerializeStringFormat(StringFormat.ISO_8859_1)
		public String stringIso;

		@Serialize
		@SerializeStringFormat(StringFormat.UTF8)
		public String stringUtf8;

		@Serialize
		@SerializeStringFormat(StringFormat.UTF8_MB3)
		public String stringUtf8Mb3;

		@Serialize
		@SerializeStringFormat(StringFormat.UTF16)
		public String stringUtf16;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestStrings that = (TestStrings) o;
			return stringIso.equals(that.stringIso) &&
					stringUtf8.equals(that.stringUtf8) &&
					stringUtf8Mb3.equals(that.stringUtf8Mb3) &&
					stringUtf16.equals(that.stringUtf16);
		}

		@Override
		public int hashCode() {
			return Objects.hash(stringIso, stringUtf8, stringUtf8Mb3, stringUtf16);
		}
	}

	public static class TestNullables {
		@Serialize
		public String @SerializeNullable [] nullArray;

		@Serialize
		public String @SerializeNullable [] notNullArray;

		@Serialize
		@SerializeNullable
		public ByteBuffer nullByteBuffer;

		@Serialize
		@SerializeNullable
		public ByteBuffer notNullByteBuffer;

		@Serialize
		@SerializeNullable
		public Collection<String> nullCollection;

		@Serialize
		@SerializeNullable
		public Collection<String> notNullCollection;

		@Serialize
		@SerializeNullable
		public TestEnum nullEnum;

		@Serialize
		@SerializeNullable
		public TestEnum notNullEnum;

		@Serialize
		@SerializeNullable
		public List<String> nullList;

		@Serialize
		@SerializeNullable
		public List<String> notNullList;

		@Serialize
		@SerializeNullable
		public Map<Integer, String> nullMap;

		@Serialize
		@SerializeNullable
		public Map<Integer, String> notNullMap;

		@Serialize
		@SerializeNullable
		public Set<String> nullSet;

		@Serialize
		@SerializeNullable
		public Set<String> notNullSet;

		@Serialize
		@SerializeNullable
		@SerializeClass(subclasses = Integer.class)
		public Object nullSubclass;

		@Serialize
		@SerializeNullable
		@SerializeClass(subclasses = Integer.class)
		public Object notNullSubclass;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestNullables that = (TestNullables) o;
			return Arrays.equals(nullArray, that.nullArray) &&
					Arrays.equals(notNullArray, that.notNullArray) &&
					Objects.equals(nullByteBuffer, that.nullByteBuffer) &&
					Objects.equals(notNullByteBuffer, that.notNullByteBuffer) &&
					Objects.equals(nullCollection, that.nullCollection) &&
					Objects.equals(new ArrayList<>(notNullCollection), new ArrayList<>(that.notNullCollection)) &&
					nullEnum == that.nullEnum &&
					notNullEnum == that.notNullEnum &&
					Objects.equals(nullList, that.nullList) &&
					Objects.equals(notNullList, that.notNullList) &&
					Objects.equals(nullMap, that.nullMap) &&
					Objects.equals(notNullMap, that.notNullMap) &&
					Objects.equals(nullSet, that.nullSet) &&
					Objects.equals(notNullSet, that.notNullSet) &&
					Objects.equals(nullSubclass, that.nullSubclass) &&
					Objects.equals(notNullSubclass, that.notNullSubclass);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(nullByteBuffer, notNullByteBuffer, nullCollection, notNullCollection,
					nullEnum, notNullEnum, nullList, notNullList, nullMap, notNullMap, nullSet, notNullSet,
					nullSubclass, notNullSubclass);
			result = 31 * result + Arrays.hashCode(nullArray);
			result = 31 * result + Arrays.hashCode(notNullArray);
			return result;
		}
	}

	public enum TestEnum {
		ONE, TWO, THREE
	}

	public static final class TestBooleans {
		@Serialize
		@SerializeNullable
		public Boolean nullBoolean;

		@Serialize
		@SerializeNullable
		public Boolean trueBoolean;

		@Serialize
		@SerializeNullable
		public Boolean falseBoolean;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestBooleans that = (TestBooleans) o;
			return Objects.equals(nullBoolean, that.nullBoolean) &&
					Objects.equals(trueBoolean, that.trueBoolean) &&
					Objects.equals(falseBoolean, that.falseBoolean);
		}

		@Override
		public int hashCode() {
			return Objects.hash(nullBoolean, trueBoolean, falseBoolean);
		}
	}
}
