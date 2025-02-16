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

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

abstract class BaseStateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef],
    extraOptions: Map[String, String] = Map.empty) extends RDD[U](dataRDD) {

  protected val storeConf = new StateStoreConf(sessionState.conf, extraOptions)

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  protected val hadoopConfBroadcast = dataRDD.context.broadcast(
    new SerializableConfiguration(sessionState.newHadoopConf()))

  /**
   * Set the preferred location of each partition using the executor that has the related
   * [[StateStoreProvider]] already loaded.
   *
   * Implementations can simply call this method in getPreferredLocations.
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val stateStoreProviderId = getStateProviderId(partition)
    storeCoordinator.flatMap(_.getLocation(stateStoreProviderId)).toSeq
  }

  protected def getStateProviderId(partition: Partition): StateStoreProviderId = {
    StateStoreProviderId(
      StateStoreId(checkpointLocation, operatorId, partition.index),
      queryRunId)
  }
}

/**
 * An RDD that allows computations to be executed against [[ReadStateStore]]s. It
 * uses the [[StateStoreCoordinator]] to get the locations of loaded state stores
 * and use that as the preferred locations.
 */
class ReadStateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeReadFunction: (ReadStateStore, Iterator[T]) => Iterator[U],
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long,
    keySchema: StructType,
    valueSchema: StructType,
    numColsPrefixKey: Int,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef],
    extraOptions: Map[String, String] = Map.empty)
  extends BaseStateStoreRDD[T, U](dataRDD, checkpointLocation, queryRunId, operatorId,
    sessionState, storeCoordinator, extraOptions) {

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    val storeProviderId = getStateProviderId(partition)

    val store = StateStore.getReadOnly(
      storeProviderId, keySchema, valueSchema, numColsPrefixKey, storeVersion,
      storeConf, hadoopConfBroadcast.value.value)
    // jgh
//    // scalastyle:off println
//    println(s"storeProviderId: ${storeProviderId}\n" +
//      s"keySchema: ${keySchema}\n" +
//      s"valueSchema: ${valueSchema}\n" +
//      s"numColsPrefixKey: ${numColsPrefixKey}\n" +
//      s"storeVersion: ${storeVersion}\n" +
//      s"storeConf: ${storeConf}\n" +
//      s"hadoopConfBroadcast.value.value: ${hadoopConfBroadcast.value.value}")
//    // scalastyle:on println
    val inputIter = dataRDD.iterator(partition, ctxt)
    // jgh
    // scalastyle:off println
//    println(s"dataRDD's String: ${dataRDD.toString} on ReadStateStoreRDD() " +
//      s"in StateStoreRDD.scala")
//    println(s"sisibalbal check Instance of dataRDD's Type[ColumnarBatchRow]: " +
//      s"${inputIter.isInstanceOf[ColumnarBatchRow]}")
    // scalastyle:on println
    storeReadFunction(store, inputIter)
  }
}

/**
 * An RDD that allows computations to be executed against [[StateStore]]s. It
 * uses the [[StateStoreCoordinator]] to get the locations of loaded state stores
 * and use that as the preferred locations.
 */
class StateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U],
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long,
    keySchema: StructType,
    valueSchema: StructType,
    numColsPrefixKey: Int,
    sessionState: SessionState,
    @transient private val storeCoordinator: Option[StateStoreCoordinatorRef],
    extraOptions: Map[String, String] = Map.empty)
  extends BaseStateStoreRDD[T, U](dataRDD, checkpointLocation, queryRunId, operatorId,
    sessionState, storeCoordinator, extraOptions) {

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    val storeProviderId = getStateProviderId(partition)

    val store = StateStore.get(
      storeProviderId, keySchema, valueSchema, numColsPrefixKey, storeVersion,
      storeConf, hadoopConfBroadcast.value.value)
    // jgh
//    // scalastyle:off println
//    println(s"storeProviderId: ${storeProviderId}\n" +
//    s"keySchema: ${keySchema}\n" +
//    s"valueSchema: ${valueSchema}\n" +
//    s"numColsPrefixKey: ${numColsPrefixKey}\n" +
//    s"storeVersion: ${storeVersion}\n" +
//    s"storeConf: ${storeConf}\n" +
//    s"hadoopConfBroadcast.value.value: ${hadoopConfBroadcast.value.value}")
//    // scalastyle:on println
    val inputIter = dataRDD.iterator(partition, ctxt)
    // jgh
    // scalastyle:off println
//    println(s"type of dataRDD: ${dataRDD.getRuntimeClassOfT} on compute() in StateStoreRDD.scala")
    // scalastyle:on println
    storeUpdateFunction(store, inputIter)
  }
}
