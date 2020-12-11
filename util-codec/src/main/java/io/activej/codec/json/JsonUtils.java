/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.codec.json;

import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.google.gson.stream.MalformedJsonException;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.util.ByteBufWriter;
import io.activej.codec.*;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UncheckedException;

import java.io.*;

public class JsonUtils {

	public static <T> T fromJson(StructuredDecoder<T> decoder, String string) throws MalformedDataException {
		JsonReader reader = new JsonReader(new StringReader(string));
		T result;
		try {
			result = decoder.decode(new JsonStructuredInput(reader));
		} catch (UncheckedException e) {
			throw e.propagate(MalformedDataException.class);
		}

		try {
			if (reader.peek() != JsonToken.END_DOCUMENT) {
				throw new MalformedDataException("Json data was not fully consumed when decoding");
			}
		} catch (EOFException | MalformedJsonException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
		return result;
	}

	private static <T> void toJson(StructuredEncoder<T> encoder, T value, Writer writer) {
		JsonWriterEx jsonWriter = new JsonWriterEx(writer);
		jsonWriter.setLenient(true);
		jsonWriter.setIndentEx("");
		jsonWriter.setHtmlSafe(false);
		jsonWriter.setSerializeNulls(true);
		encoder.encode(new JsonStructuredOutput(jsonWriter), value);
	}

	public static <T> String toJson(StructuredEncoder<? super T> encoder, T value) {
		StringWriter writer = new StringWriter();
		toJson(encoder, value, writer);
		return writer.toString();
	}

	public static <T> ByteBuf toJsonBuf(StructuredEncoder<? super T> encoder, T value) {
		ByteBufWriter writer = new ByteBufWriter();
		toJson(encoder, value, writer);
		return writer.getBuf();
	}

	/**
	 * Encodes a given value as a JSON using {@link StructuredEncoder} and appends the result to the {@link Appendable}.
	 * Passed {@link Appendable} should not perform any blocking I/O operations
	 *
	 * @param encoder structured encoder
	 * @param value a value to be encoded to JSON
	 * @param appendable nonblocking appendable where an encoded as json value will be appended
	 * @param <T> type of value to be encoded
	 */
	public static <T> void toJson(StructuredEncoder<? super T> encoder, T value, Appendable appendable) {
		toJson(encoder, value, Streams.writerForAppendable(appendable));
	}

	public static <T> StructuredCodec<T> oneline(StructuredCodec<T> codec) {
		return indent(codec, "");
	}

	public static <T> StructuredCodec<T> indent(StructuredCodec<T> codec, String indent) {
		return new StructuredCodec<T>() {
			@Override
			public void encode(StructuredOutput out, T item) {
				if (out instanceof JsonStructuredOutput) {
					JsonStructuredOutput jsonOut = ((JsonStructuredOutput) out);
					if (jsonOut.writer instanceof JsonWriterEx) {
						JsonWriterEx jsonWriterEx = (JsonWriterEx) jsonOut.writer;
						String previousIndent = jsonWriterEx.getIndentEx();
						jsonWriterEx.setIndentEx(indent);
						if (indent.isEmpty()) {
							try {
								jsonWriterEx.writer.write('\n');
							} catch (IOException e) {
								throw new AssertionError();
							}
						}
						codec.encode(out, item);
						jsonWriterEx.setIndentEx(previousIndent);
						return;
					}
				}
				codec.encode(out, item);
			}

			@Override
			public T decode(StructuredInput in) throws MalformedDataException {
				return codec.decode(in);
			}
		};
	}

	public static final class JsonWriterEx extends JsonWriter {
		final Writer writer;

		public JsonWriterEx(Writer writer) {
			super(writer);
			this.writer = writer;
		}

		private String indentEx;

		public final void setIndentEx(String indent) {
			this.indentEx = indent;
			setIndent(indent);
		}

		public final String getIndentEx() {
			return indentEx;
		}
	}

}
