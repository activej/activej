package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.TruncatedDataException;
import io.activej.csp.ChannelSupplier;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.csp.binary.ByteBufsDecoder.ofCrlfTerminatedBytes;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class BinaryChannelSupplierTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testParseStream() {
		ByteBuf buf = await(BinaryChannelSupplier.of(ChannelSupplier.of(wrapUtf8("Hello\r\n World\r\n")))
				.parseStream(ofCrlfTerminatedBytes())
				.toCollector(ByteBufQueue.collector()));
		assertEquals("Hello World", buf.asString(UTF_8));
	}

	@Test
	public void testParseStreamLessData() {
		Exception exception = awaitException(BinaryChannelSupplier.of(ChannelSupplier.of(wrapUtf8("Hello\r\n Wo")))
				.parseStream(ofCrlfTerminatedBytes())
				.toCollector(ByteBufQueue.collector()));
		assertThat(exception, instanceOf(TruncatedDataException.class));
	}
}
