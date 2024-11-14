package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.TruncatedDataException;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.csp.binary.decoder.ByteBufsDecoders.ofCrlfTerminatedBytes;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public final class BinaryChannelSupplierTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testDecodeStream() {
		ByteBuf buf = await(BinaryChannelSupplier.of(ChannelSuppliers.ofValue(wrapUtf8("Hello\r\n World\r\n")))
			.decodeStream(ofCrlfTerminatedBytes())
			.toCollector(ByteBufs.collector()));
		assertEquals("Hello World", buf.asString(UTF_8));
	}

	@Test
	public void testDecodeStreamLessData() {
		Exception exception = awaitException(BinaryChannelSupplier.of(ChannelSuppliers.ofValue(wrapUtf8("Hello\r\n Wo")))
			.decodeStream(ofCrlfTerminatedBytes())
			.toCollector(ByteBufs.collector()));
		assertThat(exception, instanceOf(TruncatedDataException.class));
	}
}
