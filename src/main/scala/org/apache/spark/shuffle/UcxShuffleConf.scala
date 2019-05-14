/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

class UcxShuffleConf(conf: SparkConf) extends SparkConf {
  private def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  val blockManagerPort = getInt("spark.blockManager.port", 0)

  val getNumProcesses = getInt("spark.executor.instances", 1)

  val coresPerProcess = getInt("spark.cores.max", 1)

  // Comma separated list of buffer size : buffer count pairs. E.g. 4k:1000,16k:500
  val preallocateBuffers: java.util.Map[java.lang.Integer, java.lang.Integer] = {
    conf.get(getUcxConf("preAllocateBuffers"), "")
      .split(",").withFilter(s => !s.isEmpty)
      .map(entry => entry.split(":") match {
        case Array(bufferSize, bufferCount) =>
          (int2Integer(Utils.byteStringAsBytes(bufferSize.trim).toInt),
            int2Integer(bufferCount.toInt))
      }).toMap.asJava
  }

  val metadataBufferSize = conf.getInt(getUcxConf("metadataBufferSize"), 4096)

  val driverHost = conf.get(getUcxConf("driver.host"),
    conf.get("spark.driver.host", "0.0.0.0"))

  val driverPort = conf.getInt(getUcxConf("driver.port"), 55443)

  val minAllocationSize = conf.getSizeAsBytes(
    getUcxConf("minAllocationSize"), 4096)

  val metadataBlockSize = conf.getSizeAsBytes(getUcxConf("metadataBlockSize"), 350)

  val numWorkers = conf.getInt(getUcxConf("numWorkers"), 1)

  val cpus = {
    val cpuArrayString = conf.get(getUcxConf("cpuList"),
      (0 to Runtime.getRuntime.availableProcessors()).mkString(","))
    cpuArrayString.split(",").map(_.toInt)
  }
}
