import io.activej.bytebuf.ByteBuf;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;

import java.util.List;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static java.nio.charset.StandardCharsets.UTF_8;

//[START EXAMPLE]
public final class ByteBufsDecoderExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop
			.builder()
			.withCurrentThread()
			.build();

		List<ByteBuf> letters = List.of(wrapAscii("H"), wrapAscii("e"), wrapAscii("l"), wrapAscii("l"), wrapAscii("o"));
		ByteBufsDecoder<String> decoder = bufs -> {
			if (!bufs.hasRemainingBytes(5)) {
				System.out.println("Not enough bytes to decode message");
				return null;
			}
			return bufs.takeExactSize(5).asString(UTF_8);
		};

		BinaryChannelSupplier.of(ChannelSuppliers.ofList(letters)).decode(decoder)
			.whenResult(x -> System.out.println(x));

		eventloop.run();
	}
}
//[END EXAMPLE]
