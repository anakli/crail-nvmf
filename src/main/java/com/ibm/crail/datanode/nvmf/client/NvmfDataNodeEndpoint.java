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

import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.DataResult;
import com.ibm.crail.namenode.protocol.BlockInfo;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public class NvmfDataNodeEndpoint implements DataNodeEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();

	public Future<DataResult> write(ByteBuffer byteBuffer, ByteBuffer byteBuffer1, BlockInfo blockInfo, long l)
			throws IOException, InterruptedException {
		return null;
	}

	public Future<DataResult> read(ByteBuffer byteBuffer, ByteBuffer byteBuffer1, BlockInfo blockInfo, long l)
			throws IOException, InterruptedException {
		return null;
	}

	public void close() throws IOException, InterruptedException {

	}

	public boolean isLocal() {
		return false;
	}
}
