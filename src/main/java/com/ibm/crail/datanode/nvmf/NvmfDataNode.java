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

package com.ibm.crail.datanode.nvmf;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.datanode.DataNodeEndpoint;
import com.ibm.crail.datanode.nvmf.client.NvmfDataNodeEndpoint;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.datanode.DataNode;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.NvmeServerEndpoint;
import com.ibm.disni.nvmef.spdk.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class NvmfDataNode extends DataNode {

	private static final Logger LOG = CrailUtils.getLogger();
	private InetSocketAddress datanodeAddr;

	public InetSocketAddress getAddress() {
		return datanodeAddr;
	}

	public void printConf(Logger logger) {
		NvmfDataNodeConstants.printConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		NvmfDataNodeConstants.updateConstants(crailConfiguration);
		NvmfDataNodeConstants.verify();
	}

	public DataNodeEndpoint createEndpoint(InetSocketAddress inetSocketAddress) throws IOException {
		return new NvmfDataNodeEndpoint();
	}

	public static InetSocketAddress getDataNodeAddress() throws Exception {
		return new InetSocketAddress(NvmfDataNodeConstants.IP_ADDR, NvmfDataNodeConstants.PORT);
	}

	public void run() throws Exception {
		LOG.info("initalizing NVMf datanode");

		NvmeEndpointGroup group = new NvmeEndpointGroup();
		NvmeServerEndpoint serverEndpoint = group.createServerEndpoint();
		URI url = new URI("nvmef://" + NvmfDataNodeConstants.IP_ADDR + ":" + NvmfDataNodeConstants.PORT +
				"/0/1?subsystem=nqn.2016-06.io.spdk:cnode1&pci=" + NvmfDataNodeConstants.PCIE_ADDR);
		serverEndpoint.bind(url);


		while (true) {
			DataNodeStatistics statistics = this.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
			Thread.sleep(2000);
		}
	}

	public void close() throws Exception {

	}
}
