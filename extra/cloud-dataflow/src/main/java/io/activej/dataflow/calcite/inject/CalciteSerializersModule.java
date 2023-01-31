package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.utils.BigDecimalBinarySerializer;
import io.activej.dataflow.calcite.utils.time.InstantBinarySerializer;
import io.activej.dataflow.calcite.utils.time.LocalDateBinarySerializer;
import io.activej.dataflow.calcite.utils.time.LocalTimeBinarySerializer;
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
		bind(new Key<BinarySerializer<LocalDate>>() {}).toInstance(LocalDateBinarySerializer.getInstance());
		bind(new Key<BinarySerializer<LocalTime>>() {}).toInstance(LocalTimeBinarySerializer.getInstance());
		bind(new Key<BinarySerializer<Instant>>() {}).toInstance(InstantBinarySerializer.getInstance());
		bind(new Key<BinarySerializer<BigDecimal>>() {}).toInstance(BigDecimalBinarySerializer.getInstance());

		bind(DefiningClassLoader.class).to(DefiningClassLoader::create);
	}
}
