package io.activej.serializer;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.types.TypeT;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static io.activej.serializer.Utils.doTest;
import static java.util.Arrays.asList;

public class RecordTest {

	@SerializeRecord
	public record Record(@SerializeNullable String s, int n) {}

	@Test
	public void testRecord() {
		{
			Record record1 = new Record("abc", 1);
			Record record2 = doTest(Record.class, record1);
			Assert.assertEquals(record1, record2);
		}
		{
			Record record1 = new Record(null, 1);
			Record record2 = doTest(Record.class, record1);
			Assert.assertEquals(record1, record2);
		}
	}

	@SuppressWarnings("ClassCanBeRecord")
	public static final class CompatiblePojo {
		private final String s;
		private final int n;

		public CompatiblePojo(@Deserialize("s") String s, @Deserialize("n") int n) {
			this.s = s;
			this.n = n;
		}

		@Serialize(order = 1)
		@SerializeNullable
		public String getS() {return s;}

		@Serialize(order = 2)
		public int getN() {return n;}
	}

	@Test
	public void testCompatiblePojo() {
		Record record = new Record("abc", 1);
		BinarySerializer<Record> serializer = SerializerBuilder.create().build(Record.class);
		BinarySerializer<CompatiblePojo> deserializer = SerializerBuilder.create().build(CompatiblePojo.class);
		CompatiblePojo pojo = doTest(record, serializer, deserializer);
		Assert.assertEquals(record.s, pojo.s);
		Assert.assertEquals(record.n, pojo.n);
	}

	@SerializeRecord
	public record GenericRecord<T>(@SerializeNullable T s, int n) {}

	@Test
	public void testGenericRecord() {
		{
			var record1 = new GenericRecord<String>("abc", 1);
			BinarySerializer<GenericRecord<String>> serializer = SerializerBuilder.create().build(new TypeT<>() {});
			var record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
		{
			var record1 = new GenericRecord<String>(null, 1);
			BinarySerializer<GenericRecord<String>> serializer = SerializerBuilder.create().build(new TypeT<>() {});
			var record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
	}

	@SerializeRecord
	public record NestedRecord(@SerializeNullable Record record) {
	}

	@Test
	public void testNestedRecords() {
		{
			Record record = new Record("abc", 1);
			NestedRecord nestedRecord1 = new NestedRecord(record);
			NestedRecord nestedRecord2 = doTest(NestedRecord.class, nestedRecord1);
			Assert.assertEquals(nestedRecord1, nestedRecord2);
		}
		{
			Record record = new Record(null, 1);
			NestedRecord nestedRecord1 = new NestedRecord(record);
			NestedRecord nestedRecord2 = doTest(NestedRecord.class, nestedRecord1);
			Assert.assertEquals(nestedRecord1, nestedRecord2);
		}
		{
			NestedRecord nestedRecord1 = new NestedRecord(null);
			NestedRecord nestedRecord2 = doTest(NestedRecord.class, nestedRecord1);
			Assert.assertEquals(nestedRecord1, nestedRecord2);
		}
	}

	@Test
	public void testRecordAnnotationCompatibilityMode() {
		BinarySerializer<Record> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.withAnnotationCompatibilityMode()
				.build(Record.class);
		{
			Record record1 = new Record("abc", 1);
			Record record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
		{
			Record record1 = new Record(null, 1);
			Record record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
	}

	@SerializeRecord
	public record RecordWithNestedNullables(@SerializeNullable List<@SerializeNullable String> list, int n) {}

	@SerializeRecord
	public record RecordWithNestedPathNullables(
			@SerializeNullable
			@SerializeNullable(path = {0}) List<String> list,
			int n) {}

	@Test
	public void testRecordsNestedNullable() {
		{
			RecordWithNestedNullables record1 = new RecordWithNestedNullables(List.of("abc", "xyz"), 1);
			RecordWithNestedNullables record2 = doTest(RecordWithNestedNullables.class, record1);
			Assert.assertEquals(record1, record2);
		}
		{
			RecordWithNestedNullables record1 = new RecordWithNestedNullables(asList("abc", null), 1);
			RecordWithNestedNullables record2 = doTest(RecordWithNestedNullables.class, record1);
			Assert.assertEquals(record1, record2);
		}
		{
			RecordWithNestedNullables record1 = new RecordWithNestedNullables(null, 1);
			RecordWithNestedNullables record2 = doTest(RecordWithNestedNullables.class, record1);
			Assert.assertEquals(record1, record2);
		}
	}

	@Test
	public void testRecordNestedNullableAnnotationCompatibilityMode() {
		BinarySerializer<RecordWithNestedPathNullables> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.withAnnotationCompatibilityMode()
				.build(RecordWithNestedPathNullables.class);
		{
			RecordWithNestedPathNullables record1 = new RecordWithNestedPathNullables(List.of("abc", "xyz"), 1);
			RecordWithNestedPathNullables record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
		{
			RecordWithNestedPathNullables record1 = new RecordWithNestedPathNullables(asList("abc", null), 1);
			RecordWithNestedPathNullables record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
		{
			RecordWithNestedPathNullables record1 = new RecordWithNestedPathNullables(null, 1);
			RecordWithNestedPathNullables record2 = doTest(record1, serializer);
			Assert.assertEquals(record1, record2);
		}
	}
}
