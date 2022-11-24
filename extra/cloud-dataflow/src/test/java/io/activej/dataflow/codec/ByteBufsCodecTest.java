package io.activej.dataflow.codec;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ByteBufsCodecTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testCodec() throws MalformedDataException {
		ByteBufsCodec<String, String> codec = Utils.codec(
				StreamCodecs.ofString(),
				StreamCodecs.ofString()
		);

		ByteBufs bufs = new ByteBufs();
		bufs.add(codec.encode("one"));
		bufs.add(codec.encode("two"));
		bufs.add(codec.encode("three"));

		ByteBuf fourBuf = codec.encode("four");
		bufs.add(fourBuf.slice(0, 2));

		assertEquals("one", codec.tryDecode(bufs));
		assertEquals("two", codec.tryDecode(bufs));
		assertEquals("three", codec.tryDecode(bufs));
		assertNull(codec.tryDecode(bufs));

		bufs.add(fourBuf.slice(2, 3));
		assertEquals("four", codec.tryDecode(bufs));

		fourBuf.recycle();
	}
}
