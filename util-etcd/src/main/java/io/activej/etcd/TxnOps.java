package io.activej.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class TxnOps {
	public final ByteSequence prefix;

	final List<Cmp> cmps;
	final List<Op> ops;

	public TxnOps(ByteSequence prefix) {
		this.prefix = prefix;
		this.cmps = new ArrayList<>();
		this.ops = new ArrayList<>();
	}

	private TxnOps(ByteSequence prefix, List<Cmp> cmps, List<Op> ops) {
		this.prefix = prefix;
		this.cmps = cmps;
		this.ops = ops;
	}

	public TxnOps child(ByteSequence suffix) {
		return new TxnOps(prefix.concat(suffix), cmps, ops);
	}

	public TxnOps child(String suffix) {
		return child(ByteSequence.from(suffix, UTF_8));
	}

	public TxnOps child(byte[] suffix) {
		return child(ByteSequence.from(suffix));
	}

	public void cmp(ByteSequence key, Cmp.Op compareOp, CmpTarget<?> target) {
		cmps.add(new Cmp(concat(key), compareOp, target));
	}

	public void put(ByteSequence key, ByteSequence value, PutOption option) {
		ops.add(Op.put(concat(key), value, option));
	}

	public void get(ByteSequence key, GetOption option) {
		ops.add(Op.get(concat(key), option));
	}

	public void delete(ByteSequence key, DeleteOption option) {
		ops.add(Op.delete(concat(key), option));
	}

	public ByteSequence concat(ByteSequence key) {
		return prefix.concat(key);
	}
}
