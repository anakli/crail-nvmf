/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.datanode.nvmf.client;

import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.DataResult;
import com.ibm.crail.datanode.nvmf.NvmfDataNodeConstants;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.utils.DirectBufferCache;
import com.ibm.disni.nvmef.NvmeEndpoint;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.spdk.IOCompletion;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public class NvmfDataNodeEndpoint implements DataNodeEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();

	private final InetSocketAddress inetSocketAddress;
	private final NvmeEndpoint endpoint;
	private final int sectorSize;
	private final DirectBufferCache cache;
	private final Semaphore commandQueueAvailable;

	public NvmfDataNodeEndpoint(NvmeEndpointGroup group, InetSocketAddress inetSocketAddress) throws IOException {
		this.inetSocketAddress = inetSocketAddress;
		endpoint = group.createEndpoint();
		try {
			URI url = new URI("nvmef://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort() +
					"/0/" + NvmfDataNodeConstants.NAMESPACE + "?subsystem=nqn.2016-06.io.spdk:cnode1");
			LOG.info("Connecting to " + url.toString());
			endpoint.connect(url);
		} catch (URISyntaxException e) {
			//FIXME
			e.printStackTrace();
		}
		sectorSize = endpoint.getSectorSize();
		cache = new DirectBufferCache();
		commandQueueAvailable = new Semaphore(endpoint.getIOQueueSize());
	}

	public int getSectorSize() {
		return sectorSize;
	}

	enum Operation {
		WRITE,
		READ;
	}

	public Future<DataResult> Op(Operation op, ByteBuffer buffer, BlockInfo remoteMr, long remoteOffset)
			throws IOException, InterruptedException {
		int length = buffer.remaining();
		if (length > CrailConstants.BLOCK_SIZE){
			throw new IOException("write size too large " + length);
		}
		if (length <= 0){
			throw new IOException("write size too small, len " + length);
		}
		if (buffer.position() < 0){
			throw new IOException("local offset too small " + buffer.position());
		}
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}

		if (remoteMr.getAddr() + remoteOffset + length > endpoint.getNamespaceSize()){
			long tmpAddr = remoteMr.getAddr() + remoteOffset + length;
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}

//		LOG.info("op = " + op.name() +
//				", position = " + buffer.position() +
//				", localOffset = " + buffer.position() +
//				", remoteOffset = " + remoteOffset +
//				", remoteAddr = " + remoteMr.getAddr() +
//				", length = " + length);

		while (commandQueueAvailable.tryAcquire()) {
			poll();
		}

		boolean aligned = NvmfDataNodeUtils.namespaceSectorOffset(sectorSize, remoteOffset) == 0
				&& NvmfDataNodeUtils.namespaceSectorOffset(sectorSize, length) == 0;
		long lba = NvmfDataNodeUtils.linearBlockAddress(remoteMr, remoteOffset, sectorSize);
		Future<DataResult> future = null;
		if (aligned) {
//			LOG.debug("aligned");
			IOCompletion completion = null;
			switch(op) {
				case READ:
					completion = endpoint.read(buffer, lba);
					break;
				case WRITE:
					completion = endpoint.write(buffer, lba);
					break;
			}
			future = new NvmfDataFuture(this, completion, length);
		} else {
			long alignedLength = NvmfDataNodeUtils.alignLength(sectorSize, remoteOffset, length);

			ByteBuffer stagingBuffer = cache.getBuffer();
			stagingBuffer.clear();
			stagingBuffer.limit((int)alignedLength);
			try {
				switch(op) {
					case READ: {
						IOCompletion completion = endpoint.read(stagingBuffer, lba);
						future = new NvmfDataUnalignedReadFuture(this, completion, buffer, remoteMr, remoteOffset, stagingBuffer);
						break;
					}
					case WRITE: {
						if (NvmfDataNodeUtils.namespaceSectorOffset(sectorSize, remoteOffset) == 0) {
							// Do not read if the offset is aligned to sector size
							int sizeToWrite = length;
							stagingBuffer.put(buffer);
							stagingBuffer.position(0);
							IOCompletion completion = endpoint.write(stagingBuffer, lba);
							future = new NvmfDataFuture(this, completion, sizeToWrite);
						} else {
							// RMW but append only file system allows only reading last sector
							// and dir entries are sector aligned
							stagingBuffer.limit(sectorSize);
							IOCompletion completion = endpoint.read(stagingBuffer, lba);
							future = new NvmfDataUnalignedRMWFuture(this, completion, buffer, remoteMr, remoteOffset, stagingBuffer);
						}
						break;
					}
				}
			} catch (NoSuchFieldException e) {
				throw new IOException(e);
			} catch (IllegalAccessException e) {
				throw new IOException(e);
			}
		}

		return future;
	}

	public Future<DataResult> write(ByteBuffer buffer, ByteBuffer region, BlockInfo blockInfo, long remoteOffset)
			throws IOException, InterruptedException {
		return Op(Operation.WRITE, buffer, blockInfo, remoteOffset);
	}

	public Future<DataResult> read(ByteBuffer buffer, ByteBuffer region, BlockInfo blockInfo, long remoteOffset)
			throws IOException, InterruptedException {
		return Op(Operation.READ, buffer, blockInfo, remoteOffset);
	}

	void poll() throws IOException {
		//TODO: 16
		endpoint.processCompletions(16);
	}

	void releaseQueueEntry() {
		commandQueueAvailable.release();
	}

	void putBuffer(ByteBuffer buffer) throws IOException {
		cache.putBuffer(buffer);
	}

	public void close() throws IOException, InterruptedException {
		endpoint.close();
	}

	public boolean isLocal() {
		return false;
	}
}
