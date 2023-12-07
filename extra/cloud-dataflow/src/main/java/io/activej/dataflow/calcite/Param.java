package io.activej.dataflow.calcite;

import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.Type;

public record Param(RexDynamicParam dynamicParam, Type paramType) {
}
