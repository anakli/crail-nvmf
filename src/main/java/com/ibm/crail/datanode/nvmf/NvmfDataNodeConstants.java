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
import com.ibm.crail.conf.CrailConstants;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NvmfDataNodeConstants {

	private final static String PREFIX = "crail.datanode.nvmf";

	public static final String IP_ADDR_KEY = "ip";
	public static InetAddress IP_ADDR;

	public static final String PORT_KEY = "port";
	public static int PORT = 50025;

	public static final String PCIE_ADDR_KEY = "pcieaddr";
	public static String PCIE_ADDR;

	public static final String NAMESPACE_KEY = "namespace";
	public static int NAMESPACE = 1;

	public static final String ALLOCATION_SIZE_KEY = "allocationsize";
	public static long ALLOCATION_SIZE = 1073741824; /* 1GB */

	private static String fullKey(String key) {
		return PREFIX + "." + key;
	}

	private static String get(CrailConfiguration conf, String key) {
		return conf.get(fullKey(key));
	}

	public static void updateConstants(CrailConfiguration conf) throws UnknownHostException {
		String arg = get(conf, PCIE_ADDR_KEY);
		if (arg != null) {
			PCIE_ADDR = arg;
		}

		arg = get(conf, NAMESPACE_KEY);
		if (arg != null) {
			NAMESPACE = Integer.parseInt(arg);
		}

		arg = get(conf, IP_ADDR_KEY);
		if (arg != null) {
			IP_ADDR = InetAddress.getByName(arg);
		}

		arg = get(conf, PORT_KEY);
		if (arg != null) {
			PORT = Integer.parseInt(arg);
		}

		arg = get(conf, ALLOCATION_SIZE_KEY);
		if (arg != null) {
			ALLOCATION_SIZE = Long.parseLong(arg);
		}
	}

	public static void verify() throws IOException {
		if (NAMESPACE <= 0){
			throw new IOException("Namespace must be > 0");
		}
		if (ALLOCATION_SIZE % CrailConstants.BLOCK_SIZE != 0){
			throw new IOException("allocationsize must be multiple of crail.blocksize");
		}
	}

	public static void printConf(Logger logger) {
		logger.info(fullKey(IP_ADDR_KEY) + " " + IP_ADDR);
		logger.info(fullKey(PORT_KEY) + " " + PORT);
		logger.info(fullKey(PCIE_ADDR_KEY) + " " + PCIE_ADDR);
		logger.info(fullKey(NAMESPACE_KEY) + " " + NAMESPACE);
		logger.info(fullKey(ALLOCATION_SIZE_KEY) + " " + ALLOCATION_SIZE);
	}
}
