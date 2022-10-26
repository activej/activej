package io.activej.dataflow.jdbc.driver.time;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.sql.Timestamp;

public class SqlTimestampJsonSerializer extends JsonSerializer<Timestamp> {
    @Override
    public void serialize(Timestamp value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeString(value.toString());
    }
}
