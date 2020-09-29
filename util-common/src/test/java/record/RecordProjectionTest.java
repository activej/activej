package record;

import io.activej.common.record.Record;
import io.activej.common.record.RecordProjection;
import io.activej.common.record.RecordScheme;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecordProjectionTest {

	@Test
	public void test() {
		RecordScheme scheme = RecordScheme.create()
				.withField("boolean", boolean.class)
				.withField("byte1", byte.class)
				.withField("byte2", byte.class)
				.withField("short1", short.class)
				.withField("short2", short.class)
				.withField("byte3", byte.class)
				.withField("int", int.class)
				.withField("long", long.class)
				.withField("float", float.class)
				.withField("double", double.class)
				.withField("byte4", byte.class);

		Record record = Record.create(scheme);
		record.put("boolean", (boolean) true);
		record.put("byte1", (byte) -1);
		record.put("byte2", (byte) 1);
		record.put("short1", (short) -2);
		record.put("short2", (short) 2);
		record.put("byte3", (byte) 4);
		record.put("int", (int) -3);
		record.put("long", (long) -4);
		record.put("float", (float) -5.5);
		record.put("double", (double) -6.5);
		record.put("byte4", (byte) 123);

		RecordProjection projection = RecordProjection.create(scheme, "boolean", "byte1", "short1", "int", "long", "double");
		Record record1 = projection.apply(record);

		//noinspection SimplifiableAssertion
		assertEquals(true, (boolean) record1.get("boolean"));
		assertEquals((byte) -1, (byte) (Byte) record1.get("byte1"));
//		assertEquals((byte) 1, (byte) (Byte) record1.get("byte2"));
		assertEquals((short) -2, (short) record1.get("short1"));
//		assertEquals((short) 2, (short) record1.get("short2"));
//		assertEquals((byte) 4, (byte) (Byte) record1.get("byte3"));
		assertEquals((int) -3, (int) record1.get("int"));
		assertEquals((long) -4, (long) record1.get("long"));
//		assertEquals((float) -5.5, (float) record1.get("float"), 1e-10);
		assertEquals((double) -6.5, (double) record1.get("double"), 1e-10);
//		assertEquals((byte) 123, (byte) (Byte) record1.get("byte4"));

	}
}
