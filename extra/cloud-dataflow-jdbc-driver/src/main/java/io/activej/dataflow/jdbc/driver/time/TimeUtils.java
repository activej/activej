package io.activej.dataflow.jdbc.driver.time;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.calcite.avatica.remote.JsonService;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public final class TimeUtils {
	public static void registerTimeSerializers(){
		SimpleModule module = new SimpleModule();

		module.addSerializer(Timestamp.class, new SqlTimestampJsonSerializer());
		module.addSerializer(Time.class, new SqlTimeJsonSerializer());
		module.addSerializer(Date.class, new SqlDateJsonSerializer());

		JsonService.MAPPER.registerModule(module);
	}
}
