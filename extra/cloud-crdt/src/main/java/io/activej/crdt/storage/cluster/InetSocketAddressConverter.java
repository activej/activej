package io.activej.crdt.storage.cluster;

import com.dslplatform.json.JsonConverter;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.StringFormatUtils;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.util.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused")
@JsonConverter(target = InetSocketAddress.class)
public abstract class InetSocketAddressConverter {

	public static final JsonReader.ReadObject<InetSocketAddress> JSON_READER = InetSocketAddressConverter::readInetSocketAddress;

	public static final JsonWriter.WriteObject<InetSocketAddress> JSON_WRITER = InetSocketAddressConverter::writeInetSocketAddress;

	private static InetSocketAddress readInetSocketAddress(JsonReader<?> reader) throws IOException {
		if (reader.wasNull()) return null;

		String addressString = reader.readString();
		try {
			return StringFormatUtils.parseInetSocketAddress(addressString);
		} catch (MalformedDataException e) {
			throw reader.newParseError(e.getMessage());
		}
	}

	private static void writeInetSocketAddress(JsonWriter writer, InetSocketAddress value) {
		if (value == null) {
			writer.writeNull();
			return;
		}
		writer.writeString(value.getAddress().getHostAddress() + ":" + value.getPort());
	}

	public static void main(String[] args) throws MalformedDataException {
		ByteBuf byteBuf = Utils.toJson(InetSocketAddress.class, new InetSocketAddress("www.google.com", 8080));
		System.out.println(byteBuf.getString(StandardCharsets.UTF_8));
		InetSocketAddress o = Utils.fromJson(InetSocketAddress.class, byteBuf);
		System.out.println(o);
	}
}
