package com.ibm.crail.datanode.nvmf.client;

import com.ibm.crail.datanode.DataResult;
import com.ibm.disni.nvmef.spdk.IOCompletion;
import com.ibm.disni.nvmef.spdk.NvmeGenericCommandStatusCode;
import com.ibm.disni.nvmef.spdk.NvmeStatusCodeType;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by jpf on 14.02.17.
 */
public class NvmfDataFuture implements Future<DataResult>, DataResult {

	private final NvmfDataNodeEndpoint endpoint;
	private final IOCompletion completion;
	private final int len;

	public NvmfDataFuture(NvmfDataNodeEndpoint endpoint, IOCompletion completion, int len) {
		this.endpoint = endpoint;
		this.completion = completion;
		this.len = len;
	}

	public int getLen() {
		return len;
	}

	public boolean cancel(boolean b) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return completion.done();
	}

	public DataResult get() throws InterruptedException, ExecutionException {
		return get(-1, null);
	}

	public DataResult get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException {
		if (!completion.done()) {
			do {
				try {
					endpoint.poll();
				} catch (IOException e) {
					throw new ExecutionException(e);
				}
			} while (!completion.done());
			if (completion.getStatusCodeType() != NvmeStatusCodeType.GENERIC &&
					completion.getStatusCode() != NvmeGenericCommandStatusCode.SUCCESS.getNumVal()) {
				throw new ExecutionException("Error: " + completion.getStatusCodeType().name() + " - " +
						completion.getStatusCode()) {
				};
			}
		}
		return this;
	}
}
