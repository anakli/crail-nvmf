package com.ibm.crail.datanode.nvmf.client;

import com.ibm.crail.datanode.DataResult;
import com.ibm.crail.datanode.nvmf.NvmfDataNodeConstants;
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
	private Exception exception;
	private boolean done;

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
		if (!done) {
			try {
				get(0, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				exception = e;
			} catch (ExecutionException e) {
				exception = e;
			} catch (TimeoutException e) {
				// i.e. operation is not finished
			}
		}
		return done;
	}

	public DataResult get() throws InterruptedException, ExecutionException {
		try {
			return get(NvmfDataNodeConstants.TIME_OUT, NvmfDataNodeConstants.TIME_UNIT);
		} catch (TimeoutException e) {
			throw new ExecutionException(e);
		}
	}

	public DataResult get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		if (!completion.done()) {
			long start = System.nanoTime();
			long end = start + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
			boolean waitTimeOut;
			do {
				try {
					endpoint.poll();
				} catch (IOException e) {
					throw new ExecutionException(e);
				}
				// we don't want to trigger timeout on first iteration
				waitTimeOut = System.nanoTime() > end;
			} while (!completion.done() && !waitTimeOut);
			if (!completion.done() && waitTimeOut) {
				throw new TimeoutException("get wait time out!");
			}
			done = true;
			endpoint.releaseQueueEntry();
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
