package io.activej.dataflow.jdbc.driver.time;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.sql.Time;

public class SqlTimeJsonSerializer extends JsonSerializer<Time> {
    @Override
    public void serialize(Time value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeString(value.toString());
    }
}
