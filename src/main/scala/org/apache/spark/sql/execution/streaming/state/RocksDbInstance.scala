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

import java.io.File
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.rocksdb._
import org.rocksdb.RocksDB
import org.rocksdb.util.SizeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class RocksDbInstance(keySchema: StructType, valueSchema: StructType, identifier: String)
    extends Logging {

  import RocksDbInstance._
  RocksDB.loadLibrary()

  var db: RocksDB = null
  protected var dbPath: String = _
  protected val readOptions: ReadOptions = new ReadOptions()
  protected val writeOptions: WriteOptions = new WriteOptions()
  protected val table_options = new BlockBasedTableConfig
  protected val options: Options = new Options()
  protected val dbStats = new Statistics()
  dbStats.setStatsLevel(StatsLevel.ALL)
  protected val bloomFilter = new BloomFilter(10, false)
  //protected val rateLimiter = new RateLimiter(1024000)

  def isOpen(): Boolean = {
    db != null
  }

  def open(path: String, conf: Map[String, String],
           readOnly: Boolean, bulkUpload: Boolean = false): Unit = {
    require(db == null, "Another rocksDb instance is already active")
    try {
      setOptions(conf, bulkUpload)
      db = if (readOnly) {
        options.setCreateIfMissing(false)
        RocksDB.openReadOnly(options, path)
      } else {
        options.setCreateIfMissing(true)
        RocksDB.open(options, path)
      }
      dbPath = path
    } catch {
      case e: Throwable =>
        throw new IllegalStateException(
          s"Error while creating rocksDb instance ${e.getMessage}",
          e)
    }
  }

  def get(key: UnsafeRow): UnsafeRow = {
    require(isOpen(), "Open rocksDb instance before any operation")
    Option(db.get(readOptions, key.getBytes)) match {
      case Some(valueInBytes) =>
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueInBytes, valueInBytes.length)
        value
      case None => null
    }
  }

  def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    db.put(key.getBytes, value.getBytes)
  }

  def remove(key: UnsafeRow): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    db.delete(key.getBytes)
  }

  def commit(backupPath: Option[String] = None): Unit = {
    backupPath.foreach(f => createCheckpoint(db, f))
  }

  def abort: Unit = {
    // no-op
  }

  def close(): Unit = {
    logDebug("Closing the db")
    try {
      dbStats.close()
      db.close()
    } finally {
      db = null
      options.close()
      readOptions.close()
      writeOptions.close()
      bloomFilter.close()
    }
  }

  def printMemoryStats(db: RocksDB): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    logDebug("Inside printMemoryStats")
    val usage = MemoryUtil.getApproximateMemoryUsageByType(
    //  List(db).asJava, Set(lRUCache.asInstanceOf[Cache]).asJava).asScala
      List(db).asJava, Set.empty[Cache].asJava).asScala

    logInfo(s"""
               | size-all-mem-tables = ${db.getProperty(db.getDefaultColumnFamily,"rocksdb.size-all-mem-tables")}
               | cur-size-all-mem-tables = ${db.getProperty(db.getDefaultColumnFamily,"rocksdb.cur-size-all-mem-tables")}
               | rocksdb.num-running-compactions = ${db.getProperty(db.getDefaultColumnFamily,"rocksdb.num-running-compactions")}
               | rocksdb.estimate-table-readers-mem = ${db.getProperty(db.getDefaultColumnFamily,"rocksdb.estimate-table-readers-mem")}
               | rocksdb.estimate-num-keys = ${db.getProperty(db.getDefaultColumnFamily,"rocksdb.estimate-num-keys")}
               | ApproximateMemoryUsageByType = ${usage.toString}
               | """.stripMargin)
  }

  def iterator(closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {
    require(isOpen(), "Open rocksDb instance before any operation")
    Option(db.getSnapshot) match {
      case Some(snapshot) =>
        logDebug(s"Inside rockdDB iterator function")
        var snapshotReadOptions: ReadOptions = new ReadOptions().setSnapshot(snapshot).setFillCache(false)
        val itr = db.newIterator(snapshotReadOptions)
        createUnsafeRowPairIterator(itr, snapshotReadOptions, snapshot, closeDbOnCompletion)
      case None =>
        Iterator.empty
    }
  }

  protected def createUnsafeRowPairIterator(
      itr: RocksIterator,
      itrReadOptions: ReadOptions,
      snapshot: Snapshot,
      closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {

    itr.seekToFirst()

    new Iterator[UnsafeRowPair] {
      @volatile var isClosed = false
      override def hasNext: Boolean = {
        if (!isClosed && itr.isValid) {
          true
        } else {
          if (!isClosed) {
            isClosed = true
            itrReadOptions.close()
            logInfo("releasing Snapshot")
            db.releaseSnapshot(snapshot)
            logInfo("Closing Iterator now")
             printMemoryStats(db)
            if (closeDbOnCompletion) {
              close()
            }
            itr.close()
            logDebug(s"read from DB completed")
          }
          false
        }
      }

      override def next(): UnsafeRowPair = {
        val keyBytes = itr.key
        val key = new UnsafeRow(keySchema.fields.length)
        key.pointTo(keyBytes, keyBytes.length)
        val valueBytes = itr.value
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueBytes, valueBytes.length)
        itr.next()
        new UnsafeRowPair(key, value)
      }
    }
  }

  def printStats: Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    try {
      val stats = db.getProperty("rocksdb.stats")
      logInfo(s"Stats = $stats")
    } catch {
      case e: Exception =>
        logWarning("Exception while getting stats")
    }
  }

  def setOptions(conf: Map[String, String], bulkUpload: Boolean): Unit = {

    // Read options
    readOptions.setFillCache(false)

    // Write options
    writeOptions.setSync(false)
    writeOptions.setDisableWAL(true)

    val dataBlockSize = conf
      .getOrElse(
        "spark.sql.streaming.stateStore.rocksDb.blockSizeInKB".toLowerCase(Locale.ROOT),
        "16")
      .toInt

    /*
    val metadataBlockSize = conf
      .getOrElse(
        "spark.sql.streaming.stateStore.rocksDb.metadataBlockSizeInKB".toLowerCase(Locale.ROOT),
        "4")
      .toInt
     */

    // Table configs
    // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
    table_options
      .setBlockSize(dataBlockSize * 1024)
      // .setMetadataBlockSize(metadataBlockSize)
      .setFilterPolicy(bloomFilter)
      .setPartitionFilters(true)
      .setIndexType(IndexType.kBinarySearch)
      .setNoBlockCache(true)
    //  .setIndexType(IndexType.kTwoLevelIndexSearch)
    //  .setBlockCache(lRUCache)
      .setCacheIndexAndFilterBlocks(false)
      .setPinTopLevelIndexAndFilter(false)
      .setCacheIndexAndFilterBlocksWithHighPriority(false)
      .setPinL0FilterAndIndexBlocksInCache(false)
      .setFormatVersion(4) // https://rocksdb.org/blog/2019/03/08/format-version-4.html
      .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)

    /*
    var bufferNumber = conf
      .getOrElse(
        "spark.sql.streaming.stateStore.rocksDb.bufferNumber".toLowerCase(Locale.ROOT),
        "5")
      .toInt

    bufferNumber = Math.max(bufferNumber, 3)

    val bufferNumberToMaintain = Math.max(bufferNumber - 2, 3)

    logInfo(
      s"Using Max Buffer Name = $bufferNumber & " +
        s"max buffer number to maintain = $bufferNumberToMaintain")
    */

    // options.setMemtablePrefixBloomSizeRatio(0.2)
    // options.useCappedPrefixExtractor(4)
    // options.prepareForBulkLoad()

    options
      .setCreateIfMissing(true)
      .setMaxWriteBufferNumber(4)
      .setMaxWriteBufferNumberToMaintain(3)
      .setMaxBackgroundCompactions(2)
      .setMaxBackgroundFlushes(2)
      .setMaxOpenFiles(8)
     // .setMaxSubcompactions(4)
      .setIncreaseParallelism(4)
      .setWriteBufferSize(128 * SizeUnit.MB)
     // .setTargetFileSizeBase(128 * SizeUnit.MB)
     // .setLevelZeroFileNumCompactionTrigger(8)
      .setLevelZeroSlowdownWritesTrigger(20)
      .setLevelZeroStopWritesTrigger(40)
      // .setMaxBytesForLevelBase(2 * SizeUnit.GB)
      .setTableFormatConfig(table_options)
      .setStatistics(dbStats)
      .setStatsDumpPeriodSec(30)
      .setBytesPerSync(1048576)
      .setCompressionType(CompressionType.NO_COMPRESSION)
    //  .setUseDirectIoForFlushAndCompaction(true)
    //  .setUseDirectReads(true)
    //  .setRateLimiter(new RateLimiter(1024000))
      .setDisableAutoCompactions(true)

    if (bulkUpload) {
      options.prepareForBulkLoad()
    }

  }

  def createCheckpoint(rocksDb: RocksDB, dir: String): Unit = {
    require(isOpen(), "Open rocksDb instance before any operation")
    val (result, elapsedMs) = Utils.timeTakenMs {
      val c = Checkpoint.create(rocksDb)
      val f: File = new File(dir)
      if (f.exists()) {
        FileUtils.deleteDirectory(f)
      }
      c.createCheckpoint(dir)
      c.close()
    }
    logDebug(s"Creating createCheckpoint at $dir took $elapsedMs ms.")
  }
}

class OptimisticTransactionDbInstance(
    keySchema: StructType,
    valueSchema: StructType,
    identifier: String)
    extends RocksDbInstance(keySchema: StructType, valueSchema: StructType, identifier: String) {

  import RocksDbInstance._
  RocksDB.loadLibrary()

  var otdb: OptimisticTransactionDB = null
  var txn: Transaction = null

  override def isOpen(): Boolean = {
    otdb != null
  }

  def open(path: String, conf: Map[String, String]): Unit = {
    open(path, conf, false)
  }

  override def open(path: String, conf: Map[String, String], readOnly: Boolean, bulkUpload: Boolean = false): Unit = {
    require(otdb == null, "Another OptimisticTransactionDbInstance instance is already active")
    require(readOnly == false, "Cannot open OptimisticTransactionDbInstance in Readonly mode")
    try {
      setOptions(conf, bulkUpload)
      options.setCreateIfMissing(true)
      otdb = OptimisticTransactionDB.open(options, path)
      db = otdb.getBaseDB
      dbPath = path
      printMemoryStats(otdb.asInstanceOf[RocksDB])
    } catch {
      case e: Throwable =>
        throw new IllegalStateException(
          s"Error while creating OptimisticTransactionDb instance" +
            s" ${e.getMessage}",
          e)
    }
  }

  def startTransactions(): Unit = {
    require(isOpen(), "Open OptimisticTransactionDbInstance before performing any operation")
    Option(txn) match {
      case None =>
        val optimisticTransactionOptions = new OptimisticTransactionOptions()
        txn = otdb.beginTransaction(writeOptions, optimisticTransactionOptions)
        txn.setSavePoint()
      case Some(x) =>
        throw new IllegalStateException(s"Already started a transaction")
    }
  }

  override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    require(txn != null, "Start Transaction before inserting any key")
    txn.put(key.getBytes, value.getBytes)
  }

  override def remove(key: UnsafeRow): Unit = {
    require(txn != null, "Start Transaction before deleting any key")
    txn.delete(key.getBytes)
  }

  override def get(key: UnsafeRow): UnsafeRow = {
    require(txn != null, "Start Transaction before fetching any key-value")
    Option(txn.get(readOptions, key.getBytes)) match {
      case Some(valueInBytes) =>
        val value = new UnsafeRow(valueSchema.fields.length)
        value.pointTo(valueInBytes, valueInBytes.length)
        value
      case None =>
        null
    }
  }

  override def commit(backupPath: Option[String] = None): Unit = {
    require(txn != null, "Start Transaction before fetching any key-value")
    // printTrxStats
    try {
      val file = new File(dbPath, identifier.toUpperCase(Locale.ROOT))
      file.createNewFile()
      logInfo("Committing the open TransactionalDB. Will close the txn")
      printMemoryStats(otdb.asInstanceOf[RocksDB])
      txn.commit()
      txn.close()
      txn = null
     // backupPath.foreach(f => createCheckpoint(otdb.asInstanceOf[RocksDB], f))
    } catch {
      case e: Exception =>
        log.error(s"Unable to commit the transactions. Error message = ${e.getMessage}")
        throw e
    }
  }

  def printTrxStats(): Unit = {
    require(txn != null, "No open Transaction")
    logInfo(s"""
               | deletes = ${txn.getNumDeletes}
               | numKeys = ${txn.getNumKeys}
               | puts =  ${txn.getNumPuts}
               | time =  ${txn.getElapsedTime}
       """.stripMargin)
  }

  override def abort(): Unit = {
    require(txn != null, "No Transaction to abort")
    txn.rollbackToSavePoint()
    txn.close()
    txn = null
  }

  override def close(): Unit = {
    require(isOpen(), "No DB to close")
    // require(txn == null, "Transaction should be closed before closing the DB connection")
    logInfo("Closing the transactional DB")
    printMemoryStats(otdb.asInstanceOf[RocksDB])
    logDebug("Closing the transaction db")
    try {
      // otdb.flush(new FlushOptions())
      dbStats.close()
      otdb.close()
      db.close()
      otdb = null
      db = null
    } finally {
      // rateLimiter.close()
      options.close()
      readOptions.close()
      writeOptions.close()
      bloomFilter.close()
    }
  }

  override def iterator(closeDbOnCompletion: Boolean): Iterator[UnsafeRowPair] = {
    require(txn != null, "Transaction is not set")
    require(
      closeDbOnCompletion == false,
      "Cannot close a DB without aborting/committing the transactions")
    val snapshot = db.getSnapshot
    val readOptions = new ReadOptions().setSnapshot(snapshot).setFillCache(false)
    logInfo("Creating Iterator now")
    printMemoryStats(otdb.asInstanceOf[RocksDB])
    val itr: RocksIterator = txn.getIterator(readOptions)
    Option(itr) match {
      case Some(i) =>
        logDebug(s"creating iterator from transaction DB")
        createUnsafeRowPairIterator(i, readOptions, snapshot, false)
      case None =>
        Iterator.empty
    }
  }

  def bulkUpload(rocksDbInstance: RocksDbInstance): Unit = {
    logInfo(s"Inside bulkUpload")
    Option(rocksDbInstance.db.getSnapshot) match {
      case Some(snapshot) =>
        logInfo(s"Inside bulkUpload")
        val snapshotReadOptions: ReadOptions = new ReadOptions().setSnapshot(snapshot).setFillCache(false)
        val itr = rocksDbInstance.db.newIterator(snapshotReadOptions)
        itr.seekToFirst()
        @volatile var isClosed = false
        while(!isClosed) {
          if (!isClosed && itr.isValid) {
            txn.put(itr.key(), itr.value())
          } else {
            isClosed = true
            commit()
            snapshotReadOptions.close()
            logInfo("releasing Snapshot")
            rocksDbInstance.db.releaseSnapshot(snapshot)
            logInfo("Closing Iterator now")
            printMemoryStats(db)
            itr.close()
          }
        }
      case None =>
        throw new IllegalStateException(
          s"Error while bulkUpload. Could not create snapshot")
    }
  }

}

object RocksDbInstance {

  RocksDB.loadLibrary()

  private val destroyOptions: Options = new Options()

  //val lRUCache = new LRUCache(1024L * 1024 * 1024, 6, false, 0.5)

  def destroyDB(path: String): Unit = {
    val f: File = new File(path)
    if (f.exists()) {
      RocksDB.destroyDB(path, destroyOptions)
      FileUtils.deleteDirectory(f)
    }
  }

}

