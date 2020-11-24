package io.activej.redis.api;

import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;

import static io.activej.common.Checks.checkNotNull;

public final class GeoradiusResult {
	private final Charset charset;
	private final byte[] member;
	@Nullable
	private final Coordinate coord;
	@Nullable
	private final Double dist;
	@Nullable
	private final Long hash;

	public GeoradiusResult(Charset charset, byte[] member, @Nullable Coordinate coord, @Nullable Double dist, @Nullable Long hash) {
		this.charset = charset;
		this.member = member;
		this.coord = coord;
		this.dist = dist;
		this.hash = hash;
	}

	public Charset getCharset() {
		return charset;
	}

	public String getMember() {
		return new String(member, charset);
	}

	public byte[] getMemberAsBinary() {
		return member;
	}

	public boolean hasCoord(){
		return coord != null;
	}

	public Coordinate getCoord() {
		return checkNotNull(coord);
	}

	public boolean hasDist(){
		return dist != null;
	}

	public Double getDist() {
		return checkNotNull(dist);
	}

	public boolean hasHash(){
		return dist != null;
	}

	public Long getHash() {
		return checkNotNull(hash);
	}

	@Override
	public String toString() {
		return "GeoradiusResult{" +
				"member=" + getMember() +
				(coord != null ? ", coord=" + coord : "") +
				(dist != null ? ", dist=" + dist : "") +
				(hash != null ? ", hash=" + hash : "") +
				'}';
	}
}
