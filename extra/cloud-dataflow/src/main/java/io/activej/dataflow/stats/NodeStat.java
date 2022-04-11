package io.activej.dataflow.stats;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "$type")
public abstract class NodeStat {
}
