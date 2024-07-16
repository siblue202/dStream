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

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Future => JFuture}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.StaticSQLConf.SQL_EVENT_TRUNCATE_LENGTH
import org.apache.spark.util.Utils

object SQLExecution {
  // *********************************dStream*********************************
  // jgh

  // Version with a input row size of Operator. so, dim2_row is number of input rows of Operator
  // scan is defined by the number of input rows. so it isn't nessesary
  // dim1_prevDevice: beforeCPU-CPU(0), beforeGPU-CPU(1), beforeCPU-GPU(2), beforeGPU-GPU(3)
  // dim2_row: 1000(0), 10000(1), 100000(2), 1000000(3)

  // val scanThroughputTable = Array.fill[Double](4, 4)(0)
//  val filterEMATable = Array.fill[Double](4, 4)(0)
//  val projectEMATable = Array.fill[Double](4, 4)(0)
//  val expandEMATable = Array.fill[Double](4, 4)(0)
//  val hashAggregateEMATable = Array.fill[Double](4, 4)(0)
//  val hashJoinEMATable = Array.fill[Double](4, 4)(0)
//  val shuffleEMATable = Array.fill[Double](4, 4)(0)
//  val sortEMATable = Array.fill[Double](4, 4)(0)
//  val stateRestoreEMATable = Array.fill[Double](4, 4)(0)
//  val stateSaveEMATable = Array.fill[Double](4, 4)(0)

  // Version with a data size of MicroBatch. so, dim2_row is read Size of MicroBatch
  // dim1_prevDevice:
  // label 0: beforeCPU-CPU(0), beforeGPU-CPU(1), beforeCPU-GPU(2), beforeGPU-GPU(3)
  // label 1: beforeCPU-CPU(4), beforeGPU-CPU(5), beforeCPU-GPU(6), beforeGPU-GPU(7) until label 5
  // dim2_row: 1kb(0), 5kb(1), 10kb(2), 50kb(3), 100kb(4),
  //           100kb(5), 1mb(6), 5mb(7), 10mb(8), higher than 10mb(9)
  val filterEMATable = Array.fill[Double](16, 30)(0)
  val projectEMATable = Array.fill[Double](16, 30)(0)
  val expandEMATable = Array.fill[Double](16, 30)(0)
  val hashAggregateEMATable = Array.fill[Double](16, 30)(0)
  val hashJoinEMATable = Array.fill[Double](16, 30)(0)
//  val shuffleEMATable = Array.fill[Double](16, 30)(0)
  val sortEMATable = Array.fill[Double](16, 30)(0)
  val exchangeEMATable = Array.fill[Double](16, 30)(0)
  val stateRestoreEMATable = Array.fill[Double](16, 30)(0)
  val stateSaveEMATable = Array.fill[Double](16, 30)(0)

  // Difficult to predict exact ROWs in the PLAN phase
  // So, we use the previously transferred ROW to calculate the transfer cost.
  val estimateRowsFilterTable = Array.fill[Double](16, 30)(0)
  val estimateRowsProjectTable = Array.fill[Double](16, 30)(0)
  val estimateRowsExpandTable = Array.fill[Double](16, 30)(0)
  val estimateRowsHashAggregateTable = Array.fill[Double](16, 30)(0)
  val estimateRowsHashJoinTable = Array.fill[Double](16, 30)(0)
//  val estimateRowsShuffleTable = Array.fill[Double](16, 30)(0)
  val estimateRowsSortTable = Array.fill[Double](16, 30)(0)
  val estimateRowsExchangeTable = Array.fill[Double](16, 30)(0)
  val estimateRowsStateRestoreTable = Array.fill[Double](16, 30)(0)
  val estimateRowsStateSaveTable = Array.fill[Double](16, 30)(0)
  // *********************************dStream*********************************

  val EXECUTION_ID_KEY = "spark.sql.execution.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains(IS_TESTING.key)

  private[sql] def checkSQLExecutionId(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    // only throw an exception during tests. a missing execution ID should not fail a job.
    if (testing && sc.getLocalProperty(EXECUTION_ID_KEY) == null) {
      // Attention testers: when a test fails with this exception, it means that the action that
      // started execution of a query didn't call withNewExecutionId. The execution ID should be
      // set by calling withNewExecutionId in the action that begins execution, like
      // Dataset.collect or DataFrameWriter.insertInto.
      throw new IllegalStateException("Execution ID should be set")
    }
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  def withNewExecutionId[T](
      queryExecution: QueryExecution,
      name: Option[String] = None)(body: => T): T = queryExecution.sparkSession.withActive {
    val sparkSession = queryExecution.sparkSession
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    val executionId = SQLExecution.nextExecutionId
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
    executionIdToQueryExecution.put(executionId, queryExecution)
    try {
      // sparkContext.getCallSite() would first try to pick up any call site that was previously
      // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
      // streaming queries would give us call site like "run at <unknown>:0"
      val callSite = sc.getCallSite()

      val truncateLength = sc.conf.get(SQL_EVENT_TRUNCATE_LENGTH)

      val desc = Option(sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION))
        .filter(_ => truncateLength > 0)
        .map { sqlStr =>
          val redactedStr = Utils
            .redact(sparkSession.sessionState.conf.stringRedactionPattern, sqlStr)
          redactedStr.substring(0, Math.min(truncateLength, redactedStr.length))
        }.getOrElse(callSite.shortForm)

      val planDescriptionMode =
        ExplainMode.fromString(sparkSession.sessionState.conf.uiExplainMode)

      withSQLConfPropagated(sparkSession) {
        var ex: Option[Throwable] = None
        val startTime = System.nanoTime()
        // jgh
        // scalastyle:off println
//        println(s"**********************************************************************")
//        println(s"SQLExecution.withNewExecutionId's executionId: ${executionId.toString}")
//        println(s"SQLExecution.withNewExecutionId's sparkPlanInfo: " +
//          s"${queryExecution.explainString(planDescriptionMode)}")
        // scalastyle:on println
        try {
          sc.listenerBus.post(SparkListenerSQLExecutionStart(
            executionId = executionId,
            description = desc,
            details = callSite.longForm,
            physicalPlanDescription = queryExecution.explainString(planDescriptionMode),
            // `queryExecution.executedPlan` triggers query planning. If it fails, the exception
            // will be caught and reported in the `SparkListenerSQLExecutionEnd`
            sparkPlanInfo = SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan),
            time = System.currentTimeMillis()))
          body
        } catch {
          case e: Throwable =>
            ex = Some(e)
            throw e
        } finally {
          val endTime = System.nanoTime()
          val event = SparkListenerSQLExecutionEnd(executionId, System.currentTimeMillis())
          // Currently only `Dataset.withAction` and `DataFrameWriter.runCommand` specify the `name`
          // parameter. The `ExecutionListenerManager` only watches SQL executions with name. We
          // can specify the execution name in more places in the future, so that
          // `QueryExecutionListener` can track more cases.
          event.executionName = name
          event.duration = endTime - startTime
          event.qe = queryExecution
          event.executionFailure = ex
          // jgh
          // scalastyle:off println
//          println(s"SQLExecution id&name: ${executionId.toString}&${name}")
          // scalastyle:on println
          sc.listenerBus.post(event)
        }
      }
    } finally {
      // jgh
      // scalastyle:off println
//      println(s"**********************************************************************")
      // scalastyle:on println
      executionIdToQueryExecution.remove(executionId)
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
    }
  }

  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastExchangeExec.relationFuture`.
   */
  def withExecutionId[T](sparkSession: SparkSession, executionId: String)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    withSQLConfPropagated(sparkSession) {
      try {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
        body
      } finally {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
      }
    }
  }

  /**
   * Wrap an action with specified SQL configs. These configs will be propagated to the executor
   * side via job local properties.
   */
  def withSQLConfPropagated[T](sparkSession: SparkSession)(body: => T): T = {
    val sc = sparkSession.sparkContext
    // Set all the specified SQL configs to local properties, so that they can be available at
    // the executor side.
    val allConfigs = sparkSession.sessionState.conf.getAllConfs
    val originalLocalProps = allConfigs.collect {
      case (key, value) if key.startsWith("spark") =>
        val originalValue = sc.getLocalProperty(key)
        sc.setLocalProperty(key, value)
        (key, originalValue)
    }

    try {
      body
    } finally {
      for ((key, value) <- originalLocalProps) {
        sc.setLocalProperty(key, value)
      }
    }
  }

  /**
   * Wrap passed function to ensure necessary thread-local variables like
   * SparkContext local properties are forwarded to execution thread
   */
  def withThreadLocalCaptured[T](
      sparkSession: SparkSession, exec: ExecutorService) (body: => T): JFuture[T] = {
    val activeSession = sparkSession
    val sc = sparkSession.sparkContext
    val localProps = Utils.cloneProperties(sc.getLocalProperties)
    exec.submit(() => {
      val originalSession = SparkSession.getActiveSession
      val originalLocalProps = sc.getLocalProperties
      SparkSession.setActiveSession(activeSession)
      sc.setLocalProperties(localProps)
      val res = body
      // reset active session and local props.
      sc.setLocalProperties(originalLocalProps)
      if (originalSession.nonEmpty) {
        SparkSession.setActiveSession(originalSession.get)
      } else {
        SparkSession.clearActiveSession()
      }
      res
    })
  }
}
