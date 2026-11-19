// BloomFilterSpark.scala
// Assignment 2: Implementing a Distributed Bloom Filter using Spark RDDs
// Run from Spark Shell with:  :load BloomFilterSpark.scala
//
// Notation (same as the assignment and the course slides):
//   m = expected number of items      p = maximum false positive probability
//   R = maximum records per partition n = number of bits     k = number of hash functions

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.util.hashing.MurmurHash3
import java.util.UUID
import java.nio.charset.StandardCharsets

// ==========================================================
// HELPERS: timing and reading the user inputs
// ==========================================================

// Runs a block of code, prints how long it took, and returns its result.
def timed[T](label: String)(block: => T): T = {
  val start = System.nanoTime()
  val result = block
  val ms = (System.nanoTime() - start) / 1e6
  println(f"[time] $label%-40s $ms%10.1f ms")
  result
}

// Reads one value from the keyboard. Pressing Enter keeps the default.
def readOrDefault(prompt: String, default: String): String = {
  print(prompt + " [default " + default + "]: ")
  val line = scala.io.StdIn.readLine()
  if (line == null || line.trim.isEmpty) default else line.trim
}

// ==========================================================
// USER INPUTS
// ==========================================================

println("\n========== USER INPUTS ==========")
val m: Long           = readOrDefault("Expected number of items (m)", "1000000").toLong
val p: Double         = readOrDefault("Maximum false positive probability (p)", "0.01").toDouble
val R: Long           = readOrDefault("Maximum records per partition (R)", "100000").toLong
// Defaults of the last two inputs follow m: extra = 1.5 * m, queries = min(m, 100000)
val extraItems: Long  = readOrDefault("Extra items for the overflow test (Task 4)", (m * 3 / 2).toString).toLong
val testQueries: Long = readOrDefault("Number of test queries", math.min(m, 100000L).toString).toLong

require(m > 0, "m must be positive")
require(p > 0.0 && p < 1.0, "p must be between 0 and 1")
require(R > 0, "R must be positive")

// ==========================================================
// PARAMETER COMPUTATION (n, k, partitions)
// ==========================================================

// n = ceil( -m * ln(p) / (ln 2)^2 )   -> smallest bit array that keeps FPR <= p
def optimalBits(items: Long, fpp: Double): Long =
  math.ceil(-items * math.log(fpp) / (math.log(2) * math.log(2))).toLong

// k = round( (n / m) * ln 2 )          -> number of hash functions that minimises FPR
def optimalHashes(bits: Long, items: Long): Int =
  math.max(1, math.round(bits.toDouble / items * math.log(2)).toInt)

// partitions = ceil( m / R )           -> no partition holds more than R records
def numPartitions(items: Long, perPartition: Long): Int =
  math.max(1, math.ceil(items.toDouble / perPartition).toInt)

// Theoretical FPR after inserting x items: (1 - e^(-k x / n))^k
def theoreticalFpr(bits: Long, k: Int, inserted: Long): Double =
  math.pow(1 - math.exp(-k.toDouble * inserted / bits), k)

val n: Long        = optimalBits(m, p)
val k: Int         = optimalHashes(n, m)
val partitions: Int = numPartitions(m, R)

println("\n========== BLOOM FILTER PARAMETERS ==========")
println("m (expected items)            : " + m)
println("p (target FPR)                : " + p)
println("R (max records per partition) : " + R)
println("n (bits)                      : " + n + f"  (${n / 8.0 / 1024 / 1024}%.2f MB, ${n.toDouble / m}%.2f bits per item)")
println(f"k exact = (n/m) ln 2          : ${n.toDouble / m * math.log(2)}%.3f")
println("k (hash functions, rounded)   : " + k)
println("Number of partitions          : " + partitions)
println(f"Theoretical FPR at m items    : ${theoreticalFpr(n, k, m)}%.5f")

// Safety check: R is "records per partition", not "number of partitions".
// A very small R creates thousands of tiny partitions and the run becomes extremely slow.
val maxPartitions = math.max(numPartitions(m, R), math.ceil(extraItems.toDouble / R).toInt)
if (maxPartitions > 20 * sc.defaultParallelism) {
  println("WARNING: R = " + R + " gives up to " + maxPartitions + " partitions for " + sc.defaultParallelism + " cores.")
  println("         R is the number of RECORDS per partition. Try R = m / (number of cores), e.g. 100000 for m = 1000000.")
}

// ==========================================================
// BIT VECTOR AND HASH FUNCTIONS
// The bit array is an Array[Long]: bit i lives in word i/64, at position i%64.
// ==========================================================

def newBits(bits: Long): Array[Long] = new Array[Long](((bits + 63) / 64).toInt)

def setBit(words: Array[Long], i: Long): Unit =
  words((i >>> 6).toInt) |= (1L << (i & 63))

def getBit(words: Array[Long], i: Long): Boolean =
  (words((i >>> 6).toInt) & (1L << (i & 63))) != 0L

// Bitwise OR of b into a (used to merge the partial filters of the partitions).
def orInto(a: Array[Long], b: Array[Long]): Array[Long] = {
  var j = 0
  while (j < a.length) {
    a(j) = a(j) | b(j)
    j += 1
  }
  a
}

// Number of bits that are 1 (used for the fill ratio).
def countOnes(words: Array[Long]): Long =
  words.iterator.map(w => java.lang.Long.bitCount(w).toLong).sum

// Double hashing (Kirsch-Mitzenmacher): g_i(x) = (h1 + i * h2) mod n, i = 0..k-1.
// Two MurmurHash3 values with different seeds simulate k independent hash functions.
def indexes(item: String, numHashes: Int, bits: Long): Array[Long] = {
  val h1 = MurmurHash3.stringHash(item, 17).toLong & 0xFFFFFFFFL
  val h2 = MurmurHash3.stringHash(item, 31).toLong & 0xFFFFFFFFL
  Array.tabulate(numHashes)(i => (h1 + i * h2) % bits)
}

// One Bloom filter layer. count = how many items have been inserted into it.
case class BloomLayer(capacity: Long, fpp: Double, n: Long, k: Int, bits: Array[Long], count: Long)

// Lookup in one layer: false as soon as one of the k bits is 0.
def layerContains(layer: BloomLayer, item: String): Boolean =
  indexes(item, layer.k, layer.n).forall(i => getBit(layer.bits, i))

// Lookup over all layers: true = "may be present", false = "definitely absent".
def mightContain(layers: Array[BloomLayer], item: String): Boolean =
  layers.exists(layer => layerContains(layer, item))

// DISTRIBUTED INSERTION
// Each partition fills its own local bit vector (no shuffle of the items),
// then treeReduce merges the partial vectors with bitwise OR.
def buildBits(items: RDD[String], bits: Long, numHashes: Int): Array[Long] = {
  val numWords = ((bits + 63) / 64).toInt
  val partial: RDD[Array[Long]] = items.mapPartitions { it =>
    val local = new Array[Long](numWords)
    it.foreach(x => indexes(x, numHashes, bits).foreach(i => setBit(local, i)))
    Iterator(local)
  }
  partial.treeReduce((a, b) => orInto(a, b), 2)
}

// Deterministic unique item: the same i always gives the same UUID string.
// RDDs below are persisted with MEMORY_AND_DISK: partitions that do not fit in memory
// are written to local disk instead of failing or being recomputed (important for large tests).
def itemName(prefix: String, i: Long): String =
  UUID.nameUUIDFromBytes((prefix + "-" + i).getBytes(StandardCharsets.UTF_8)).toString

// Prints the number of records in every partition of an RDD.
def partitionStats(label: String, rdd: RDD[String], limit: Long): Unit = {
  val sizes = rdd.mapPartitionsWithIndex((idx, it) => Iterator((idx, it.size.toLong))).collect()
  println("\n" + label + ": " + sizes.length + " partitions")
  sizes.foreach { case (idx, size) => println(f"  partition $idx%3d : $size%,d records") }
  println(f"  min = ${sizes.map(_._2).min}%,d   max = ${sizes.map(_._2).max}%,d   limit R = $limit%,d")
  println("  every partition <= R : " + sizes.forall(_._2 <= limit))
}

// ==========================================================
// TASK 1: CREATE A LARGE SET OF UNIQUE ITEMS
// ==========================================================

println("\n========== TASK 1: DATASET ==========")

val dataset: RDD[String] =
  sc.range(0, m, 1, partitions).map(i => itemName("item", i)).persist(StorageLevel.MEMORY_AND_DISK)

val datasetCount: Long = timed("Generate dataset + count")(dataset.count())
val distinctCount: Long = timed("Check uniqueness (distinct count)")(dataset.distinct().count())

println("Items generated  : " + datasetCount)
println("Distinct items   : " + distinctCount)
println("Example items    : " + dataset.take(3).mkString(", "))
partitionStats("Dataset partitioning", dataset, R)

// ==========================================================
// TASK 2: DISTRIBUTED BLOOM FILTER (insertion)
// ==========================================================

println("\n========== TASK 2: INSERTION ==========")

val bits0: Array[Long] = timed("Distributed insertion (mapPartitions + OR)")(buildBits(dataset, n, k))
val layer0 = BloomLayer(m, p, n, k, bits0, datasetCount)
val onesLayer0: Long = countOnes(bits0)

var filterBC = timed("Broadcast bit vector to executors")(sc.broadcast(Array(layer0)))

println("Bits set to 1    : " + onesLayer0 + " of " + n)
println(f"Fill ratio       : ${onesLayer0.toDouble / n}%.4f   (ideal for an optimal filter = 0.5)")

// ==========================================================
// TASK 3: LOOKUP FACILITY
// ==========================================================

println("\n========== TASK 3: LOOKUP ==========")

// Single lookups on the driver
println("Single lookups:")
dataset.take(3).foreach(x => println("  " + x + " (inserted)     -> " + mightContain(filterBC.value, x)))
Seq(itemName("absent", -1), itemName("absent", -2), "hello-world").foreach { x =>
  println("  " + x + " (not inserted) -> " + mightContain(filterBC.value, x))
}

// Batch lookups on the executors.
// Members: a random sample of the dataset. Non-members: items with a different prefix.
val memberFraction = math.min(1.0, testQueries.toDouble / datasetCount)
val members: RDD[String] = dataset.sample(false, memberFraction, 42L)
val nonMembers: RDD[String] =
  sc.range(0, testQueries, 1, numPartitions(testQueries, R)).map(i => itemName("absent", i))

val memberTests: Long = members.count()
val falseNegatives: Long = timed("Lookup of members (batch)") {
  val bc = filterBC
  members.filter(x => !mightContain(bc.value, x)).count()
}

val lookupStart = System.nanoTime()
val falsePositives: Long = timed("Lookup of non-members (batch)") {
  val bc = filterBC
  nonMembers.filter(x => mightContain(bc.value, x)).count()
}
val lookupSeconds = (System.nanoTime() - lookupStart) / 1e9

val observedFpr = falsePositives.toDouble / testQueries

println("Members tested      : " + memberTests + "   false negatives = " + falseNegatives + "  (must be 0)")
println("Non-members tested  : " + testQueries + "   false positives = " + falsePositives)
println(f"Observed FPR        : $observedFpr%.5f")
println(f"Theoretical FPR     : ${theoreticalFpr(n, k, datasetCount)}%.5f")
println(f"Target p            : $p%.5f")
println(f"Lookup throughput   : ${testQueries / lookupSeconds}%,.0f queries/second")

// ==========================================================
// TASK 4: HANDLING CAPACITY OVERFLOW (Scalable Bloom Filter)
// ==========================================================

println("\n========== TASK 4: OVERFLOW ==========")

val growth: Double = 2.0     // s: each new layer has twice the capacity of the previous one
val tightening: Double = 0.5 // r: each new layer has half the error of the previous one

val extra: RDD[String] =
  sc.range(m, m + extraItems, 1, numPartitions(extraItems, R)).map(i => itemName("item", i)).persist(StorageLevel.MEMORY_AND_DISK)

println("Extra items to insert : " + extra.count() + "  (total = " + (datasetCount + extraItems) + " = " + f"${(datasetCount + extraItems).toDouble / m}%.1f" + " x m)")

// (a) The problem: a fixed filter that keeps receiving items.
val overloadedBits: Array[Long] = orInto(buildBits(extra, n, k), bits0)
val overloaded = BloomLayer(m, p, n, k, overloadedBits, datasetCount + extraItems)
val overloadedFp: Long = {
  val bc = sc.broadcast(Array(overloaded))
  val fp = nonMembers.filter(x => mightContain(bc.value, x)).count()
  bc.destroy()
  fp
}
println("\n(a) Fixed filter overloaded (the problem):")
println(f"    fill ratio = ${countOnes(overloadedBits).toDouble / n}%.4f   observed FPR = ${overloadedFp.toDouble / testQueries}%.5f   theoretical = ${theoreticalFpr(n, k, datasetCount + extraItems)}%.5f")

// (b) The solution: add new layers. Old layers are never rebuilt.
// New items fill the remaining capacity of the active (last) layer first;
// when it is full, a new layer i is created with capacity m*s^i and error p*r^i.
def addBatch(layers: Array[BloomLayer], batch: RDD[String]): Array[BloomLayer] = {
  val total = batch.count()
  val indexed: RDD[(String, Long)] = batch.zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK)
  var current = layers
  var offset = 0L
  while (offset < total) {
    var active = current.last
    if (active.count >= active.capacity) {
      val i = current.length
      val cap = (m * math.pow(growth, i)).toLong
      val fpp = p * math.pow(tightening, i)
      val bitsI = optimalBits(cap, fpp)
      val kI = optimalHashes(bitsI, cap)
      active = BloomLayer(cap, fpp, bitsI, kI, newBits(bitsI), 0L)
      current = current :+ active
      println(f"    new layer $i: capacity = $cap%,d  p_i = $fpp%.4f  n = $bitsI%,d bits  k = $kI")
    }
    val take = math.min(active.capacity - active.count, total - offset)
    val from = offset
    val until = offset + take
    val slice: RDD[String] = indexed.filter { case (_, idx) => idx >= from && idx < until }.map(_._1)
    val merged = orInto(buildBits(slice, active.n, active.k), active.bits)
    current = current.updated(current.length - 1, active.copy(bits = merged, count = active.count + take))
    offset = until
  }
  indexed.unpersist()
  current
}

println("\n(b) Scalable Bloom filter (the solution):")
val scalableLayers: Array[BloomLayer] = timed("Insert extra items into new layer(s)")(addBatch(Array(layer0), extra))

// Proof that the original data was not reindexed: layer 0 is unchanged.
println("    layer 0 unchanged (no reindexing) : " + (countOnes(scalableLayers(0).bits) == onesLayer0))

filterBC.destroy()
filterBC = timed("Re-broadcast all layers")(sc.broadcast(scalableLayers))

scalableLayers.zipWithIndex.foreach { case (layer, i) =>
  println(f"    layer $i: items = ${layer.count}%,d / ${layer.capacity}%,d   k = ${layer.k}   fill = ${countOnes(layer.bits).toDouble / layer.n}%.4f   FPR_i = ${theoreticalFpr(layer.n, layer.k, layer.count)}%.5f")
}

val allItems: RDD[String] = dataset.union(extra)
val allMembers: RDD[String] = allItems.sample(false, math.min(1.0, 2.0 * testQueries / (datasetCount + extraItems)), 7L)
val allMemberTests: Long = allMembers.count()
val scalableFn: Long = { val bc = filterBC; allMembers.filter(x => !mightContain(bc.value, x)).count() }
val scalableFp: Long = timed("Lookup of non-members (all layers)") {
  val bc = filterBC
  nonMembers.filter(x => mightContain(bc.value, x)).count()
}
val theoreticalScalable = 1 - scalableLayers.map(l => 1 - theoreticalFpr(l.n, l.k, l.count)).product
val upperBound = p / (1 - tightening)

println("    members tested     : " + allMemberTests + "   false negatives = " + scalableFn + "  (must be 0)")
println(f"    observed FPR       : ${scalableFp.toDouble / testQueries}%.5f")
println(f"    theoretical FPR    : $theoreticalScalable%.5f   (1 - prod(1 - FPR_i))")
println(f"    upper bound p/(1-r): $upperBound%.5f")

// ==========================================================
// SUMMARY
// ==========================================================

println("\n========== SUMMARY ==========")
println(f"n = $n%,d bits, k = $k, partitions = $partitions")
println(f"Base filter        : observed FPR = $observedFpr%.5f   false negatives = $falseNegatives")
println(f"Fixed, overloaded  : observed FPR = ${overloadedFp.toDouble / testQueries}%.5f")
println(f"Scalable (${scalableLayers.length} layers): observed FPR = ${scalableFp.toDouble / testQueries}%.5f   false negatives = $scalableFn")

// ==========================================================
// INTERACTIVE LOOKUP LOOP
//   <word>        -> look the word up in all layers
//   add <word>    -> insert the word into the active (last) layer
//   exit          -> stop
// Runs on the driver, so you can see which bits are checked for each word.
// ==========================================================

println("\n========== INTERACTIVE LOOKUP ==========")
println("Type a word to look it up, 'add <word>' to insert it, or 'exit' to stop.")
println("Example items that ARE in the filter: " + dataset.take(2).mkString(", "))

var liveLayers: Array[BloomLayer] = scalableLayers
var running = true

while (running) {
  print("\n> ")
  val input = scala.io.StdIn.readLine()

  if (input == null || input.trim == "exit") {
    running = false
  } else if (input.trim.startsWith("add ")) {
    // Several words on one line (separated by spaces or commas) are inserted as separate items.
    val words = input.trim.substring(4).trim.split("[\\s,]+").filter(_.nonEmpty)
    val last = liveLayers.last
    words.foreach(word => indexes(word, last.k, last.n).foreach(i => setBit(last.bits, i)))
    liveLayers = liveLayers.updated(liveLayers.length - 1, last.copy(count = last.count + words.length))
    println("  inserted " + words.length + " item(s) into layer " + (liveLayers.length - 1) + ": " + words.mkString(", "))
  } else if (input.trim.nonEmpty) {
    // Several words on one line (separated by spaces or commas) are looked up one by one.
    input.trim.split("[\\s,]+").filter(_.nonEmpty).foreach { word =>
      println("  '" + word + "':")
      liveLayers.zipWithIndex.foreach { case (layer, i) =>
        val checks = indexes(word, layer.k, layer.n).map(pos => s"$pos=${if (getBit(layer.bits, pos)) 1 else 0}")
        println("    layer " + i + " bits checked: " + checks.mkString(" ") + "  -> " + layerContains(layer, word))
      }
      if (mightContain(liveLayers, word)) {
        println("    RESULT: MAY BE PRESENT (all k bits are 1 in at least one layer)")
      } else {
        println("    RESULT: DEFINITELY ABSENT (a 0 bit was found in every layer)")
      }
    }
  }
}

println("\n========== ASSIGNMENT COMPLETED ==========")