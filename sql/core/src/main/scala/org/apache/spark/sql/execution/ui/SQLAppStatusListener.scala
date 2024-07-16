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
package org.apache.spark.sql.execution.ui

import java.io.{File, FileOutputStream}
import java.util.{Arrays, Date, NoSuchElementException}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Status._
import org.apache.spark.scheduler._
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.status.{ElementTrackingStore, KVUtils, LiveEntity}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashMap


class SQLAppStatusListener(
                            conf: SparkConf,
                            kvstore: ElementTrackingStore,
                            live: Boolean) extends SparkListener with Logging {

  // **************************** dStream ****************************
  // id, planMetricCollector
  private val execCollector = mutable.Map[Long, mutable.Map[SparkPlanInfo, Seq[SQLMetricInfo]]]()
  // id, [operName, order of oper]
  private val execLabel: mutable.Map[String, Int] = mutable.Map("filter" -> -1, "project" -> -1,
    "aggregate" -> -1, "join" -> -1, "sort" -> -1, "expand" -> -1, "exchange" -> -1)
  // exec, [accId of prevOutputRowsAccumulator, value]
  // rowCollector is not used. so comment it
  //  private val prevRowsCollector = mutable.Map[Long, mutable.Map[Long, Long]]()
  val metricFileName = "/data/result/" + String.valueOf(System.currentTimeMillis() / 1000) + ".csv"
  val resultFileOut: FileOutputStream = new FileOutputStream(new File(metricFileName))

  // scalastyle:off println
  Console.withOut(resultFileOut) {
    println("executionID", "SQLlabel", "operatorName", "prevOutputRows",
      "metricName", "metricValue")
  }
  // scalastyle:on println
  // *****************************************************************

  // How often to flush intermediate state of a live execution to the store. When replaying logs,
  // never flush (only do the very last write).
  private val liveUpdatePeriodNs = if (live) conf.get(LIVE_ENTITY_UPDATE_PERIOD) else -1L

  // Live tracked data is needed by the SQL status store to calculate metrics for in-flight
  // executions; that means arbitrary threads may be querying these maps, so they need to be
  // thread-safe.
  private val liveExecutions = new ConcurrentHashMap[Long, LiveExecutionData]()
  private val stageMetrics = new ConcurrentHashMap[Int, LiveStageMetrics]()

  // Returns true if this listener has no live data. Exposed for tests only.
  private[sql] def noLiveData(): Boolean = {
    liveExecutions.isEmpty && stageMetrics.isEmpty
  }

  kvstore.addTrigger(classOf[SQLExecutionUIData], conf.get[Int](UI_RETAINED_EXECUTIONS)) { count =>
    cleanupExecutions(count)
  }

  kvstore.onFlush {
    if (!live) {
      val now = System.nanoTime()
      liveExecutions.values.asScala.foreach { exec =>
        // This saves the partial aggregated metrics to the store; this works currently because
        // when the SHS sees an updated event log, all old data for the application is thrown
        // away.
        exec.metricsValues = aggregateMetrics(exec)
        exec.write(kvstore, now)
      }
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString = event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    val jobId = event.jobId
    val exec = Option(liveExecutions.get(executionId))
      .orElse {
        try {
          // Should not overwrite the kvstore with new entry, if it already has the SQLExecution
          // data corresponding to the execId.
          val sqlStoreData = kvstore.read(classOf[SQLExecutionUIData], executionId)
          val executionData = new LiveExecutionData(executionId)
          executionData.description = sqlStoreData.description
          executionData.details = sqlStoreData.details
          executionData.physicalPlanDescription = sqlStoreData.physicalPlanDescription
          executionData.metrics = sqlStoreData.metrics
          executionData.submissionTime = sqlStoreData.submissionTime
          executionData.completionTime = sqlStoreData.completionTime
          executionData.jobs = sqlStoreData.jobs
          executionData.stages = sqlStoreData.stages
          executionData.metricsValues = sqlStoreData.metricValues
          executionData.endEvents.set(sqlStoreData.jobs.size + 1)
          liveExecutions.put(executionId, executionData)
          Some(executionData)
        } catch {
          case _: NoSuchElementException => None
        }
      }.getOrElse(getOrCreateExecution(executionId))

    // Record the accumulator IDs and metric types for the stages of this job, so that the code
    // that keeps track of the metrics knows which accumulators to look at.
    val accumIdsAndType = exec.metrics.map { m => (m.accumulatorId, m.metricType) }.toMap
    if (accumIdsAndType.nonEmpty) {
      event.stageInfos.foreach { stage =>
        stageMetrics.put(stage.stageId, new LiveStageMetrics(stage.stageId, 0,
          stage.numTasks, accumIdsAndType))
      }
    }

    exec.jobs = exec.jobs + (jobId -> JobExecutionStatus.RUNNING)
    exec.stages ++= event.stageIds.toSet
    update(exec, force = true)
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    if (!isSQLStage(event.stageInfo.stageId)) {
      return
    }

    // Reset the metrics tracking object for the new attempt.
    Option(stageMetrics.get(event.stageInfo.stageId)).foreach { stage =>
      if (stage.attemptId != event.stageInfo.attemptNumber) {
        stageMetrics.put(event.stageInfo.stageId,
          new LiveStageMetrics(event.stageInfo.stageId, event.stageInfo.attemptNumber,
            stage.numTasks, stage.accumIdsToMetricType))
      }
    }
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    liveExecutions.values().asScala.foreach { exec =>
      if (exec.jobs.contains(event.jobId)) {
        val result = event.jobResult match {
          case JobSucceeded => JobExecutionStatus.SUCCEEDED
          case _ => JobExecutionStatus.FAILED
        }
        exec.jobs = exec.jobs + (event.jobId -> result)
        exec.endEvents.incrementAndGet()
        update(exec)
      }
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    event.accumUpdates.foreach { case (taskId, stageId, attemptId, accumUpdates) =>
      updateStageMetrics(stageId, attemptId, taskId, SQLAppStatusListener.UNKNOWN_INDEX,
        accumUpdates, false)
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    Option(stageMetrics.get(event.stageId)).foreach { stage =>
      if (stage.attemptId == event.stageAttemptId) {
        stage.registerTask(event.taskInfo.taskId, event.taskInfo.index)
      }
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    if (!isSQLStage(event.stageId)) {
      return
    }

    val info = event.taskInfo
    // SPARK-20342. If processing events from a live application, use the task metrics info to
    // work around a race in the DAGScheduler. The metrics info does not contain accumulator info
    // when reading event logs in the SHS, so we have to rely on the accumulator in that case.
    val accums = if (live && event.taskMetrics != null) {
      event.taskMetrics.externalAccums.flatMap { a =>
        // This call may fail if the accumulator is gc'ed, so account for that.
        try {
          Some(a.toInfo(Some(a.value), None))
        } catch {
          case _: IllegalAccessError => None
        }
      }
    } else {
      info.accumulables
    }
    updateStageMetrics(event.stageId, event.stageAttemptId, info.taskId, info.index, accums.toSeq,
      info.successful)
  }

  def liveExecutionMetrics(executionId: Long): Option[Map[Long, String]] = {
    Option(liveExecutions.get(executionId)).map { exec =>
      if (exec.metricsValues != null) {
        exec.metricsValues
      } else {
        aggregateMetrics(exec)
      }
    }
  }

  private def aggregateMetrics(exec: LiveExecutionData): Map[Long, String] = {
    val accumIds = exec.metrics.map(_.accumulatorId).toSet

    val metricAggregationMap = new mutable.HashMap[String, (Array[Long], Array[Long]) => String]()
    val metricAggregationMethods = exec.metrics.map { m =>
      val optClassName = CustomMetrics.parseV2CustomMetricType(m.metricType)
      val metricAggMethod = optClassName.map { className =>
        if (metricAggregationMap.contains(className)) {
          metricAggregationMap(className)
        } else {
          // Try to initiate custom metric object
          try {
            val metric = Utils.loadExtensions(classOf[CustomMetric], Seq(className), conf).head
            val method =
              (metrics: Array[Long], _: Array[Long]) => metric.aggregateTaskMetrics(metrics)
            metricAggregationMap.put(className, method)
            method
          } catch {
            case NonFatal(_) =>
              // Cannot initialize custom metric object, we might be in history server that does
              // not have the custom metric class.
              val defaultMethod = (_: Array[Long], _: Array[Long]) => "N/A"
              metricAggregationMap.put(className, defaultMethod)
              defaultMethod
          }
        }
      }.getOrElse(
        // Built-in SQLMetric
        SQLMetrics.stringValue(m.metricType, _, _)
      )
      (m.accumulatorId, metricAggMethod)
    }.toMap

    val liveStageMetrics = exec.stages.toSeq
      .flatMap { stageId => Option(stageMetrics.get(stageId)) }

    val taskMetrics = liveStageMetrics.flatMap(_.metricValues())

    val maxMetrics = liveStageMetrics.flatMap(_.maxMetricValues())

    val allMetrics = new mutable.HashMap[Long, Array[Long]]()

    val maxMetricsFromAllStages = new mutable.HashMap[Long, Array[Long]]()

    taskMetrics.filter(m => accumIds.contains(m._1)).foreach { case (id, values) =>
      val prev = allMetrics.getOrElse(id, null)
      val updated = if (prev != null) {
        prev ++ values
      } else {
        values
      }
      allMetrics(id) = updated
    }

    // Find the max for each metric id between all stages.
    val validMaxMetrics = maxMetrics.filter(m => accumIds.contains(m._1))
    validMaxMetrics.foreach { case (id, value, taskId, stageId, attemptId) =>
      val updated = maxMetricsFromAllStages.getOrElse(id, Array(value, stageId, attemptId, taskId))
      if (value > updated(0)) {
        updated(0) = value
        updated(1) = stageId
        updated(2) = attemptId
        updated(3) = taskId
      }
      maxMetricsFromAllStages(id) = updated
    }

    exec.driverAccumUpdates.foreach { case (id, value) =>
      if (accumIds.contains(id)) {
        val prev = allMetrics.getOrElse(id, null)
        val updated = if (prev != null) {
          // If the driver updates same metrics as tasks and has higher value then remove
          // that entry from maxMetricsFromAllStage. This would make stringValue function default
          // to "driver" that would be displayed on UI.
          if (maxMetricsFromAllStages.contains(id) && value > maxMetricsFromAllStages(id)(0)) {
            maxMetricsFromAllStages.remove(id)
          }
          val _copy = Arrays.copyOf(prev, prev.length + 1)
          _copy(prev.length) = value
          _copy
        } else {
          Array(value)
        }
        allMetrics(id) = updated
      }
    }

    val aggregatedMetrics = allMetrics.map { case (id, values) =>
      id -> metricAggregationMethods(id)(values, maxMetricsFromAllStages.getOrElse(id,
        Array.empty[Long]))
    }.toMap

    // jgh
    // scalastyle:off println
    //    println(s"exec.metricsValues: ${exec.metricsValues} on aggregateMetrics() " +
    //      s"in SQLAppStatusListener.scala")
    //    println(s"aggregatedMetrics: ${aggregatedMetrics} on aggregateMetrics() " +
    //      s"in SQLAppStatusListener.scala")

    // jgh start
    val maxPattern = """.*,\s(\d+)\s\(""".r
    //    val totalPattern = """^total.*\n(\d+)""".r
    if (!aggregatedMetrics.isEmpty) {
      val startTime = System.nanoTime()
      //      println(s"aggregatedMetrics: ${aggregatedMetrics} in SQLAppStatusListener.scala")
      val currentPlanMetricCollector = execCollector.get(exec.execID)
      // rowCollector is not used. so comment it
      //      val rowsCollector = prevRowsCollector.get(exec.execID)
      // curr top of SparkPlanInfo
      var topSparkPlanInfo: SparkPlanInfo = null

      if (currentPlanMetricCollector.isDefined) {
        topSparkPlanInfo = currentPlanMetricCollector.get.head._1
        aggregatedMetrics.foreach { case (accId, value) =>
          // rowCollector is not used. so comment it
          //          if (rowsCollector.get.contains(accId)) {
          //            rowsCollector.get += (accId -> value.replace(",", "").toLong)
          //          }
          currentPlanMetricCollector.get.foreach { case (sparkPlanInfo, metrics) =>
            if (topSparkPlanInfo.label < sparkPlanInfo.label) {
              topSparkPlanInfo = sparkPlanInfo
            }
            metrics.find(_.accumulatorId == accId).foreach { metric =>
              if (metric.name == "output rows" || metric.name == "number of output rows") {
                sparkPlanInfo.outputRows = value.replace(",", "").toLong
              } else if (metric.name.contains("time") && metric.name != "stream time" &&
                metric.name != "aggregation time") {
                //                val totalValue = totalPattern.findFirstMatchIn(value) match {
                //                  case Some(matched) => matched.group(1).toLong
                //                  case None => 0
                //                }
                val maxValue = maxPattern.findFirstMatchIn(value) match {
                  case Some(matched) => matched.group(1).toLong
                  case None => 0
                }
                sparkPlanInfo.duration = sparkPlanInfo.duration + maxValue
                //                sparkPlanInfo.duration = sparkPlanInfo.duration + totalValue
                if (sparkPlanInfo.nodeName.contains("Filter")) {
                  println(s"[${sparkPlanInfo.nodeName}][${sparkPlanInfo.label}]'s duration: " +
                    s"${sparkPlanInfo.duration} && ${maxValue} in SQLAppStatusListener.scala")
                }
              }
              metric.valueString = value
            }
      //            println(s"[${sparkPlanInfo.nodeName}]'s duration: ${sparkPlanInfo.duration} " +
      //              s"in SQLAppStatusListener.scala")
          }
        }
      }
      // update sparkPlanInfo's inputRows
      topSparkPlanInfo.children.foreach(_.propagate())
      val aggEndTime = System.nanoTime()

      // for time test
      val updateStartTime = System.nanoTime()

      if (currentPlanMetricCollector.isDefined) {
        currentPlanMetricCollector.get.foreach { case (sparkPlanInfo, metrics) =>
          val duration: Double = sparkPlanInfo.duration match {
//            case a if a > 0 => a/1000
            case a if a > 0 => a * 0.001
            case _ => 1
          }
          //        val throughput = sparkPlanInfo.outputRows / sparkPlanInfo.duration
          val order = sparkPlanInfo.orderOfOper
          val dimZero: Int = sparkPlanInfo.children.headOption match {
            case Some(child) =>
              if (child.nodeName.contains("GpuRowToColumnar")) 2 + order * 4
              else if (child.nodeName.contains("GpuColumnarToRow")) 1 + order * 4
              else if (child.nodeName.contains("Gpu") && sparkPlanInfo.nodeName.contains("Gpu")) {
                3 + order * 4
              }
              else 0 + order * 4
            case _ => 0
          }
          val dimOne: Int = sparkPlanInfo.dataSizeOfMB match {
//            case a if a < 1024 => 0     // 1KB
//            case b if b < 5120 => 1     // 5KB
//            case c if c < 10240 => 2    // 10KB
//            case d if d < 51200 => 3    // 50KB
//            case e if e < 102400 => 4   // 100KB
//            case f if f < 512000 => 5   // 500KB
//            case g if g < 1048576 => 6  // 1MB
//            case h if h < 5242880 => 7  // 5MB
//            case i if i < 10485760 => 8 // 10MB
//            case _ => 9                 // 10MB ~
            case a if a < 1024 => 0    // 1KB
            case b if b < 5 * 1024 => 1    // 5KB
            case c if c < 10 * 1024 => 2    // 10KB
            case d if d < 20 * 1024 => 3    // 20KB
            case e if e < 30 * 1024 => 4    // 30KB
            case f if f < 40 * 1024 => 5    // 40KB
            case g if g < 50 * 1024 => 6    // 50KB
            case h if h < 60 * 1024 => 7    // 60KB
            case i if i < 70 * 1024 => 8    // 70KB
            case j if j < 80 * 1024 => 9    // 80KB
            case k if k < 90 * 1024 => 10    // 90KB
            case l if l < 100 * 1024 => 11    // 100KB
            case m if m < 200 * 1024 => 12    // 200KB
            case n if n < 300 * 1024 => 13    // 300KB
            case o if o < 400 * 1024 => 14    // 400KB
            case p if p < 500 * 1024 => 15    // 500KB
            case q if q < 600 * 1024 => 16    // 600KB
            case r if r < 700 * 1024 => 17    // 700KB
            case s if s < 800 * 1024 => 18    // 800KB
            case t if t < 900 * 1024 => 19    // 900KB
            case u if u < 1024 * 1024 => 20    // 1MB
            case v if v < 2 * 1024 * 1024 => 21    // 2MB
            case w if w < 3 * 1024 * 1024 => 22    // 3MB
            case x if x < 4 * 1024 * 1024 => 23    // 4MB
            case y if y < 5 * 1024 * 1024 => 24    // 5MB
            case z if z < 6 * 1024 * 1024 => 25    // 6MB
            case aa if aa < 7 * 1024 * 1024 => 26    // 7MB
            case bb if bb < 8 * 1024 * 1024 => 27    // 8MB
            case cc if cc < 9 * 1024 * 1024 => 28    // 9MB
            case _ => 29    // 9MB ~
          }
          val weight = 0.5
          if (sparkPlanInfo.nodeName.contains("Filter")) {
            SQLExecution.filterEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.filterEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsFilterTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("Sort")) {
            SQLExecution.sortEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.sortEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsSortTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("Project")) {
            SQLExecution.projectEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.projectEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsProjectTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("Expand")) {
            SQLExecution.expandEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.expandEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsExpandTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("HashAggregate")) {
            SQLExecution.hashAggregateEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.hashAggregateEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsHashAggregateTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("Join")) {
            SQLExecution.hashJoinEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.hashJoinEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsHashJoinTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("Exchange")) {
            SQLExecution.exchangeEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.exchangeEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsExchangeTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("StateStoreRestore")) {
            SQLExecution.stateRestoreEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.stateRestoreEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsStateRestoreTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          } else if (sparkPlanInfo.nodeName.contains("StateStoreSave")) {
            SQLExecution.stateSaveEMATable(dimZero)(dimOne) =
              (1-weight) * duration + weight * SQLExecution.stateSaveEMATable(dimZero)(dimOne)
            SQLExecution.estimateRowsStateSaveTable(dimZero)(dimOne) =
              sparkPlanInfo.inputRows
          }
        }
      }
      val aggTime = aggEndTime - startTime
      val updateTime = System.nanoTime() - updateStartTime
      println(s"agg time:${aggTime/1000} for aggregate metrics in SQLAppStatusListener.scala\n")
      println(s"update time:${updateTime/1000} for update EMA value " +
        s"in SQLAppStatusListener.scala\n")

      // write metrics
      if (currentPlanMetricCollector.isDefined) {
        currentPlanMetricCollector.get.foreach { case (sparkPlanInfo, metrics) =>
          // Write the metrics result to the file
          metrics.foreach { metric =>
            Console.withOut(resultFileOut) {
              println(
                s"${exec.execID}&${sparkPlanInfo.label}&${sparkPlanInfo.nodeName}&" +
                  s"${sparkPlanInfo.inputRows}&${metric.name}&${metric.valueString}"
              )
            }
          }
          //          println(s"Plan: ${sparkPlanInfo.nodeName}")
          //          println(s"Plan's label: ${sparkPlanInfo.label}")
          //          println(s"Plan's inputRows: ${sparkPlanInfo.inputRows}")
          //          println(s"Plan's outputRows: ${sparkPlanInfo.outputRows}")
          //          println(s"Plan's duration: ${sparkPlanInfo.duration}")
          //          println(s"Plan's throughput: ${throughput}")
          //          println(s"Plan's dim: [${dimZero}], [${dimOne}], [${dimTwo}]")
        }
      }
    }

    // scalastyle:on println
    // jgh end

    // Check the execution again for whether the aggregated metrics data has been calculated.
    // This can happen if the UI is requesting this data, and the onExecutionEnd handler is
    // running at the same time. The metrics calculated for the UI can be inaccurate in that
    // case, since the onExecutionEnd handler will clean up tracked stage metrics.
    if (exec.metricsValues != null) {
      exec.metricsValues
    } else {
      aggregatedMetrics
    }
  }

  private def updateStageMetrics(
                                  stageId: Int,
                                  attemptId: Int,
                                  taskId: Long,
                                  taskIdx: Int,
                                  accumUpdates: Seq[AccumulableInfo],
                                  succeeded: Boolean): Unit = {
    Option(stageMetrics.get(stageId)).foreach { metrics =>
      if (metrics.attemptId == attemptId) {
        metrics.updateTaskMetrics(taskId, taskIdx, succeeded, accumUpdates)
      }
    }
  }

  private def toStoredNodes(nodes: Seq[SparkPlanGraphNode]): Seq[SparkPlanGraphNodeWrapper] = {
    nodes.map {
      case cluster: SparkPlanGraphCluster =>
        val storedCluster = new SparkPlanGraphClusterWrapper(
          cluster.id,
          cluster.name,
          cluster.desc,
          toStoredNodes(cluster.nodes.toSeq),
          cluster.metrics)
        new SparkPlanGraphNodeWrapper(null, storedCluster)

      case node =>
        new SparkPlanGraphNodeWrapper(node, null)
    }
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val SparkListenerSQLExecutionStart(executionId, description, details,
    physicalPlanDescription, sparkPlanInfo, time) = event

    val planGraph = SparkPlanGraph(sparkPlanInfo)
    val sqlPlanMetrics = planGraph.allNodes.flatMap { node =>
      node.metrics.map { metric => (metric.accumulatorId, metric) }
    }.toMap.values.toList

    val graphToStore = new SparkPlanGraphWrapper(
      executionId,
      toStoredNodes(planGraph.nodes),
      planGraph.edges)
    kvstore.write(graphToStore)

    // jgh
    // scalastyle:off println
    //    println(s"exec.metrics: ${sqlPlanMetrics} on onExecutinoStart() " +
    //      s"in SQLAppStatusListener.scala\n")
    //    println(s"planGraph: ${planGraph} on onExecutionStart() in SQLAppStatusListener.scala\n")
    //    println(s"executionID: ${executionId}" +
    //      s"sparkPlanInfo: ${sparkPlanInfo} \n" +
    //      s"sparkPlanInfo's nodeName: ${sparkPlanInfo.nodeName} \n" +
    //      s"sparkPlanInfo's children: ${sparkPlanInfo.children} \n" +
    //      s"sparkPlanInfo's metrics: ${sparkPlanInfo.metrics} \n" +
    //      s"on onExecutionStart()" +
    //      s"in SQLAppStatusListener.scala\n")

    // jgh start
    // label sparkPlan
    val startTime = System.nanoTime()

    def labelNode(node: SparkPlanInfo, currentLabel: Int): Int = {
      var nextLabel = currentLabel
      node.children.foreach(child => {
        nextLabel = labelNode(child, nextLabel)
      })
      nextLabel += 1
      node.label = nextLabel

      // update the order of operator in sql plan
      val keyOfMap = node.nodeName match {
        case filter if filter.contains("Filter") => "filter"
        case project if project.contains("Project") => "project"
        case aggregate if aggregate.contains("Aggregate") => "aggregate"
        case sort if sort.contains("Sort") => "sort"
        case join if join.contains("Join") => "join"
        case expand if expand.contains("Expand") => "expand"
        case exchange if exchange.contains("exchange") => "exchange"
        case _ => "other"
      }

      execLabel.get(keyOfMap) match {
        case Some(currValue) =>
          execLabel(keyOfMap) = currValue + 1
          node.orderOfOper = execLabel(keyOfMap)
        case None =>
          node.orderOfOper = 0
      }

      node.dataSizeOfMB = SparkPlan.getCurrentMBSize
      println(s"[${node.label}][${node.nodeName}]: ${node.orderOfOper}, ${node.dataSizeOfMB}")

      nextLabel
    }

    labelNode(sparkPlanInfo, 0)

    val planMetricsCollector = mutable.Map[SparkPlanInfo, Seq[SQLMetricInfo]]()
    // rowCollector is not used. so comment it
    //    val rowsCollector = mutable.Map[Long, Long]()

    def collectMetrics(
                        plan: SparkPlanInfo,
                        collector: mutable.Map[SparkPlanInfo, Seq[SQLMetricInfo]]): Unit = {
      collector(plan) = plan.metrics
      // rowCollector is not used. so comment it
      //      for (i <- plan.metrics) {
      //        if (i.name == "output rows") {
      //          rowsCollector += (i.accumulatorId -> 0)
      //        } else if (i.name == "number of output rows") {
      //          rowsCollector += (i.accumulatorId -> 0)
      //        }
      //      }
      plan.children.foreach(child => collectMetrics(child, collector))
    }

    collectMetrics(sparkPlanInfo, planMetricsCollector)
    execCollector(executionId) = planMetricsCollector
    // rowCollector is not used. so comment it
    //    prevRowsCollector(executionId) = rowsCollector
    execLabel.transform( (k, v) => -1 )
    val prepareTime = System.nanoTime() - startTime
    println(s"prepareTime: ${prepareTime / 1000} on onExecutionStart() " +
      s"in SQLAppStatusListener.scala\n")
    // scalastyle:on println
    // jgh end

    val exec = getOrCreateExecution(executionId)
    exec.description = description
    exec.details = details
    exec.physicalPlanDescription = physicalPlanDescription
    exec.metrics = sqlPlanMetrics
    exec.submissionTime = time
    exec.execID = executionId // jgh
    // jgh
    // scalastyle:off println
    //    println(s"exec.description: ${description} on onExecutinoStart() " +
    //      s"in SQLAppStatusListener.scala\n")
    //    println(s"exec.details: ${details} on onExecutinoStart() " +
    //      s"in SQLAppStatusListener.scala\n")
    //    println(s"exec.physicalPlanDescription: ${physicalPlanDescription} on onExecutinoStart() +
    //      s"in SQLAppStatusListener.scala\n")
    //    println(s"exec.sqlPlanMetrics: ${sqlPlanMetrics} on onExecutinoStart() " +
    //      s"in SQLAppStatusListener.scala\n")
    //    println(s"exec.time: ${time} on onExecutinoStart() " +
    //      s"in SQLAppStatusListener.scala\n")
    // scalastyle:on println
    update(exec)
  }

  private def onAdaptiveExecutionUpdate(event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    val SparkListenerSQLAdaptiveExecutionUpdate(
    executionId, physicalPlanDescription, sparkPlanInfo) = event

    val planGraph = SparkPlanGraph(sparkPlanInfo)
    val sqlPlanMetrics = planGraph.allNodes.flatMap { node =>
      node.metrics.map { metric => (metric.accumulatorId, metric) }
    }.toMap.values.toList

    val graphToStore = new SparkPlanGraphWrapper(
      executionId,
      toStoredNodes(planGraph.nodes),
      planGraph.edges)
    kvstore.write(graphToStore)

    val exec = getOrCreateExecution(executionId)
    exec.physicalPlanDescription = physicalPlanDescription
    exec.metrics ++= sqlPlanMetrics
    update(exec)
  }

  private def onAdaptiveSQLMetricUpdate(event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    val SparkListenerSQLAdaptiveSQLMetricUpdates(executionId, sqlPlanMetrics) = event

    val exec = getOrCreateExecution(executionId)
    exec.metrics ++= sqlPlanMetrics
    update(exec)
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val SparkListenerSQLExecutionEnd(executionId, time) = event
    Option(liveExecutions.get(executionId)).foreach { exec =>
      exec.completionTime = Some(new Date(time))
      update(exec)

      // Aggregating metrics can be expensive for large queries, so do it asynchronously. The end
      // event count is updated after the metrics have been aggregated, to prevent a job end event
      // arriving during aggregation from cleaning up the metrics data.
      kvstore.doAsync {
        exec.metricsValues = aggregateMetrics(exec)
        removeStaleMetricsData(exec)
        exec.endEvents.incrementAndGet()
        update(exec, force = true)
      }
    }


    // jgh start
    // delete past data from execCollector
    if (executionId > 2) {
      execCollector.remove(executionId - 2)
      //      prevRowsCollector.remove(executionId - 2)
    }
    // jgh end
  }

  private def removeStaleMetricsData(exec: LiveExecutionData): Unit = {
    // Remove stale LiveStageMetrics objects for stages that are not active anymore.
    val activeStages = liveExecutions.values().asScala.flatMap { other =>
      if (other != exec) other.stages else Nil
    }.toSet
    stageMetrics.keySet().asScala
      .filter(!activeStages.contains(_))
      .foreach(stageMetrics.remove)
  }

  private def onDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Unit = {
    val SparkListenerDriverAccumUpdates(executionId, accumUpdates) = event
    Option(liveExecutions.get(executionId)).foreach { exec =>
      exec.driverAccumUpdates = exec.driverAccumUpdates ++ accumUpdates
      update(exec)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLAdaptiveExecutionUpdate => onAdaptiveExecutionUpdate(e)
    case e: SparkListenerSQLAdaptiveSQLMetricUpdates => onAdaptiveSQLMetricUpdate(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case e: SparkListenerDriverAccumUpdates => onDriverAccumUpdates(e)
    case _ => // Ignore
  }

  private def getOrCreateExecution(executionId: Long): LiveExecutionData = {
    liveExecutions.computeIfAbsent(executionId,
      (_: Long) => new LiveExecutionData(executionId))
  }

  private def update(exec: LiveExecutionData, force: Boolean = false): Unit = {
    val now = System.nanoTime()
    if (exec.endEvents.get() >= exec.jobs.size + 1) {
      exec.write(kvstore, now)
      removeStaleMetricsData(exec)
      liveExecutions.remove(exec.executionId)
    } else if (force) {
      exec.write(kvstore, now)
    } else if (liveUpdatePeriodNs >= 0) {
      if (now - exec.lastWriteTime > liveUpdatePeriodNs) {
        exec.write(kvstore, now)
      }
    }
  }

  private def isSQLStage(stageId: Int): Boolean = {
    liveExecutions.values().asScala.exists { exec =>
      exec.stages.contains(stageId)
    }
  }

  private def cleanupExecutions(count: Long): Unit = {
    val countToDelete = count - conf.get(UI_RETAINED_EXECUTIONS)
    if (countToDelete <= 0) {
      return
    }

    val view = kvstore.view(classOf[SQLExecutionUIData]).index("completionTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt)(_.completionTime.isDefined)
    toDelete.foreach { e =>
      kvstore.delete(e.getClass(), e.executionId)
      kvstore.delete(classOf[SparkPlanGraphWrapper], e.executionId)
    }
  }

}

private class LiveExecutionData(val executionId: Long) extends LiveEntity {

  var description: String = null
  var details: String = null
  var physicalPlanDescription: String = null
  var metrics = Seq[SQLPlanMetric]()
  var submissionTime = -1L
  var completionTime: Option[Date] = None

  var jobs = Map[Int, JobExecutionStatus]()
  var stages = Set[Int]()
  var driverAccumUpdates = Seq[(Long, Long)]()

  @volatile var metricsValues: Map[Long, String] = null

  var execID: Long = -1L // jgh

  // Just in case job end and execution end arrive out of order, keep track of how many
  // end events arrived so that the listener can stop tracking the execution.
  val endEvents = new AtomicInteger()

  override protected def doUpdate(): Any = {
    new SQLExecutionUIData(
      executionId,
      description,
      details,
      physicalPlanDescription,
      metrics,
      submissionTime,
      completionTime,
      jobs,
      stages,
      metricsValues)
  }

}

private class LiveStageMetrics(
                                val stageId: Int,
                                val attemptId: Int,
                                val numTasks: Int,
                                val accumIdsToMetricType: Map[Long, String]) {

  /**
   * Mapping of task IDs to their respective index. Note this may contain more elements than the
   * stage's number of tasks, if speculative execution is on.
   */
  private val taskIndices = new OpenHashMap[Long, Int]()

  /** Bit set tracking which indices have been successfully computed. */
  private val completedIndices = new mutable.BitSet()

  /**
   * Task metrics values for the stage. Maps the metric ID to the metric values for each
   * index. For each metric ID, there will be the same number of values as the number
   * of indices. This relies on `SQLMetrics.stringValue` treating 0 as a neutral value,
   * independent of the actual metric type.
   */
  private val taskMetrics = new ConcurrentHashMap[Long, Array[Long]]()

  private val metricsIdToMaxTaskValue = new ConcurrentHashMap[Long, Array[Long]]()

  def registerTask(taskId: Long, taskIdx: Int): Unit = {
    taskIndices.update(taskId, taskIdx)
  }

  def updateTaskMetrics(
                         taskId: Long,
                         eventIdx: Int,
                         finished: Boolean,
                         accumUpdates: Seq[AccumulableInfo]): Unit = {
    val taskIdx = if (eventIdx == SQLAppStatusListener.UNKNOWN_INDEX) {
      if (!taskIndices.contains(taskId)) {
        // We probably missed the start event for the task, just ignore it.
        return
      }
      taskIndices(taskId)
    } else {
      // Here we can recover from a missing task start event. Just register the task again.
      registerTask(taskId, eventIdx)
      eventIdx
    }

    if (completedIndices.contains(taskIdx)) {
      return
    }

    accumUpdates
      .filter { acc => acc.update.isDefined && accumIdsToMetricType.contains(acc.id) }
      .foreach { acc =>
        // In a live application, accumulators have Long values, but when reading from event
        // logs, they have String values. For now, assume all accumulators are Long and convert
        // accordingly.
        val value = acc.update.get match {
          case s: String => s.toLong
          case l: Long => l
          case o => throw QueryExecutionErrors.unexpectedAccumulableUpdateValueError(o)
        }

        val metricValues = taskMetrics.computeIfAbsent(acc.id, _ => new Array(numTasks))
        metricValues(taskIdx) = value

        if (SQLMetrics.metricNeedsMax(accumIdsToMetricType(acc.id))) {
          val maxMetricsTaskId = metricsIdToMaxTaskValue.computeIfAbsent(acc.id, _ => Array(value,
            taskId))

          if (value > maxMetricsTaskId.head) {
            maxMetricsTaskId(0) = value
            maxMetricsTaskId(1) = taskId
          }
        }
      }
    if (finished) {
      completedIndices += taskIdx
    }
  }

  def metricValues(): Seq[(Long, Array[Long])] = taskMetrics.asScala.toSeq

  // Return Seq of metric id, value, taskId, stageId, attemptId for this stage
  def maxMetricValues(): Seq[(Long, Long, Long, Int, Int)] = {
    metricsIdToMaxTaskValue.asScala.toSeq.map { case (id, maxMetrics) => (id, maxMetrics(0),
      maxMetrics(1), stageId, attemptId)
    }
  }
}

private object SQLAppStatusListener {
  val UNKNOWN_INDEX = -1
}
