package io.activej.serializer;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.types.TypeT;
import org.junit.Assert;
import org.junit.Test;

import static io.activej.serializer.Utils.doTest;

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
			var record2 = doTest(record1, SerializerBuilder.create().build(new TypeT<GenericRecord<String>>() {}));
			Assert.assertEquals(record1, record2);
		}
		{
			var record1 = new GenericRecord<String>(null, 1);
			var record2 = doTest(record1, SerializerBuilder.create().build(new TypeT<GenericRecord<String>>() {}));
			Assert.assertEquals(record1, record2);
		}
	}

}
