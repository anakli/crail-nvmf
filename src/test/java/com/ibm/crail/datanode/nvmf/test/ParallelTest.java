/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
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

package com.ibm.crail.datanode.nvmf.test;

import com.ibm.crail.CrailBufferedInputStream;
import com.ibm.crail.CrailBufferedOutputStream;
import com.ibm.crail.CrailFile;
import com.ibm.crail.CrailFS;
import com.ibm.crail.conf.CrailConfiguration;

import junit.framework.TestCase;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParallelTest extends TestCase {

	@Override
	protected void setUp() throws Exception {
		CrailConfiguration conf = new CrailConfiguration();
		fs = CrailFS.newInstance(conf);
		fs.makeDirectory(basePath.toString()).get();
	}

	// --------------------

	private static final Logger LOG = LoggerFactory.getLogger(ParallelTest.class);

	protected CrailFS fs;

	private final Path testBasePath = FileSystems.getDefault().getPath("/test");
	private final Path basePath = FileSystems.getDefault().getPath(testBasePath.toString(), Long.toString(System.nanoTime()));

	@Override
	protected void tearDown() throws Exception {
//		fs.delete(basePath, true);
		fs.close();
	}

	public void testParallelAppend() throws Exception {
		final int host = 0;
		final int numThreads = 4;
		final int timesAppend = 1024*10;

		final CyclicBarrier barrier = new CyclicBarrier(numThreads);
		Thread threads[] = new Thread[numThreads];
		final byte buf[][] = new byte[numThreads][];
		Random rand = new Random();
		for (int i = 0; i < threads.length; i++) {
			buf[i] = new byte[rand.nextInt(1024*1024) + 1];
			Arrays.fill(buf[i], (byte)((int)'A' + i));
		}

		for (int i = 0; i < threads.length; i++) {
			Runnable task = new Runnable() {
				private int i;

				@Override
				public void run() {
					final Path file = FileSystems.getDefault().getPath(basePath.toString(), "file" + i);
					try {
						CrailFile f = fs.createFile(file.toString(), host, 0).get();
						CrailBufferedOutputStream out = f.getBufferedOutputStream(0);
						barrier.await();
						for (int j = 0; j < timesAppend; j++) {
							out.write(buf[i]);
						}
						out.close();
					} catch (Exception e) {
						e.printStackTrace();
						assertTrue(false);
					}

				}

				public Runnable set(int i) {
					this.i = i;
					return this;
				}
			}.set(i);
			threads[i] = new Thread(task);
			threads[i].start();
		}

		for (Thread t : threads) {
			t.join();
		}

		barrier.reset();
		for (int i = 0; i < threads.length; i++) {
			Runnable task = new Runnable() {
				private int i;

				@Override
				public void run() {
					final Path file = FileSystems.getDefault().getPath(basePath.toString(), "file" + i);
					try {
						CrailFile f = fs.lookupNode(file.toString()).get().asFile();
						CrailBufferedInputStream in = f.getBufferedInputStream(0);
						final byte readData[] = new byte[buf[i].length];
						barrier.await();
						for (int j = 0; j < timesAppend; j++) {
							in.read(readData);
							assertTrue(Arrays.equals(readData, buf[i]));
						}
						in.close();
					} catch (Exception e) {
						e.printStackTrace();
						assertTrue(false);
					}

				}

				public Runnable set(int i) {
					this.i = i;
					return this;
				}
			}.set(i);
			threads[i] = new Thread(task);
			threads[i].start();
		}

		for (Thread t : threads) {
			t.join();
		}

		InputStream ms = fs.lookupNode(basePath.toString()).get().asDirectory().getMultiStream(64);
		Iterator<String> iter2 = fs.lookupNode(basePath.toString()).get().asDirectory().listEntries();
		while (iter2.hasNext()) {
			Path p = FileSystems.getDefault().getPath(iter2.next());
			int i = Integer.valueOf(p.getFileName().toString().substring(4));
			final byte readData[] = new byte[buf[i].length];
			for (int j = 0; j < timesAppend; j++) {
				ms.read(readData);
				if (!Arrays.equals(readData, buf[i])) {
					String filename = basePath.toString() + "/file" + i;
					for (int h = 0; h < buf[i].length; h++) {
						if (readData[h] != buf[i][h]) {
							LOG.info("Data read from " + filename + " is not equal to data written!\n" +
									"Differ at offset = " + h + ", read = " + Integer.toHexString((int)readData[h]) +
									", wrote = " + Integer.toHexString((int)buf[i][h]));
							break;
						}
					}

					// dump to file
					FileOutputStream outFile = new FileOutputStream("dump" + filename.replace('/', '_'));
					outFile.write(readData);
					outFile.close();
					assertTrue(false);
				}
			}
		}
	}
}

