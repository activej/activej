package io.activej.crdt.storage.cluster;

import com.dslplatform.json.JsonConverter;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import io.activej.common.StringFormatUtils;
import io.activej.common.exception.MalformedDataException;

import java.io.IOException;
import java.net.InetSocketAddress;

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
}
