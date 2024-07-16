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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.internal.SQLConf

/**
 * :: DeveloperApi ::
 * Stores information about a SQL SparkPlan.
 */
@DeveloperApi
class SparkPlanInfo(
                     val nodeName: String,
                     val simpleString: String,
                     val children: Seq[SparkPlanInfo],
                     val metadata: Map[String, String],
                     val metrics: Seq[SQLMetricInfo]) {

  var label: Int = 0 // jgh, order of sql plan
  var inputRows: Long = 0L // jgh, number of output rows of previous operator
  var outputRows: Long = -1L // jgh, number of output rows of this operator
  var duration: Long = 1L // jgh, time of execution
  var orderOfOper: Int = -1 // jgh, order of operator in sql plan
  var dataSizeOfMB: Double = 0.0 // jgh, input data size of this operator
  var flag: Boolean = false // jgh, flag for whether this operator is concated or not

  // jgh start
  // Helper recursive method to propagate parent's outputRows or inputRows to children's inputRows
  def propagate(): Unit = {
    children.foreach(_.propagate())

    if (children.nonEmpty) {
      inputRows = children.map(_.outputRows).sum
    }
    // if outputRows is not set, set it to inputRows
    if (inputRows < 0) {
      inputRows = children.map(_.inputRows).sum
    }
    if (outputRows == -1) {
      outputRows = inputRows
    }

    children.headOption match {
      case Some(child) =>
        if (child.nodeName.contains("GpuCoalesceBatches") ||
          child.nodeName.contains("GpuShuffleCoalesce") ||
          child.nodeName.contains("GpuRowToColumnar") ||
          child.nodeName.contains("GpuColumnarToRow")) {
          flag = true
        }
      case None =>
    }

    if (flag) {
      duration = duration + children.map(_.duration).sum
      flag = false
    }
  }
  // jgh end

  override def hashCode(): Int = {
    // hashCode of simpleString should be good enough to distinguish the plans from each other
    // within a plan
    simpleString.hashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: SparkPlanInfo =>
      nodeName == o.nodeName && simpleString == o.simpleString && children == o.children
    case _ => false
  }
}

private[execution] object SparkPlanInfo {

  def fromSparkPlan(plan: SparkPlan): SparkPlanInfo = {
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case ReusedSubqueryExec(child) => child :: Nil
      case a: AdaptiveSparkPlanExec => a.executedPlan :: Nil
      case stage: QueryStageExec => stage.plan :: Nil
      case inMemTab: InMemoryTableScanExec => inMemTab.relation.cachedPlan :: Nil
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    // dump the file scan metadata (e.g file path) to event log
    val metadata = plan match {
      case fileScan: FileSourceScanExec => fileScan.metadata
      case _ => Map[String, String]()
    }
    new SparkPlanInfo(
      plan.nodeName,
      plan.simpleString(SQLConf.get.maxToStringFields),
      children.map(fromSparkPlan),
      metadata,
      metrics)
  }
}
