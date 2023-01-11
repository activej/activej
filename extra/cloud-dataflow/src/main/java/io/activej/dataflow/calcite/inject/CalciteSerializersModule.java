package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.utils.BinarySerializer_BigDecimal;
import io.activej.dataflow.calcite.utils.time.BinarySerializer_Instant;
import io.activej.dataflow.calcite.utils.time.BinarySerializer_LocalDate;
import io.activej.dataflow.calcite.utils.time.BinarySerializer_LocalTime;
import io.activej.inject.Key;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.BinarySerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

public final class CalciteSerializersModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(new Key<BinarySerializer<LocalDate>>() {}).toInstance(BinarySerializer_LocalDate.getInstance());
		bind(new Key<BinarySerializer<LocalTime>>() {}).toInstance(BinarySerializer_LocalTime.getInstance());
		bind(new Key<BinarySerializer<Instant>>() {}).toInstance(BinarySerializer_Instant.getInstance());
		bind(new Key<BinarySerializer<BigDecimal>>() {}).toInstance(BinarySerializer_BigDecimal.getInstance());

		bind(DefiningClassLoader.class).to(DefiningClassLoader::create);
	}
}
