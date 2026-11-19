Assignment 2 Report: Distributed Bloom Filter using Spark RDDs

Script: BloomFilterSpark.scala   ·   Notation as in the assignment: m = items, p = target FPR, R = records per partition, n = bits, k = hash functions.
1. Parameter computation
The program reads m, p and R from the keyboard (Enter keeps the default) and derives the three parameters:
n = ⌈ −m · ln p / (ln 2)² ⌉        k = round( (n/m) · ln 2 )        partitions = ⌈ m / R ⌉
n is the smallest bit array that keeps the false positive rate at or below p. k is the number of hash functions that minimises the FPR for that n. ⌈m/R⌉ guarantees that no partition (node) holds more than R records. The expected FPR after inserting x items is FPR(x) = (1 − e^(−kx/n))^k.
Quantity	Value	Note
Inputs m / p / R	1,000,000 / 0.01 / 100,000	defaults of the script
n (bits)	9,585,059	1.14 MB, 9.59 bits per item
k (hash functions)	7	exact value 6.644, rounded
Partitions	10	⌈1,000,000 / 100,000⌉
Theoretical FPR at m items	1.004%	(1 − e^(−7·m/n))^7
2. Implementation
Task 1 – Dataset. sc.range(0, m, 1, partitions) creates m indices spread over the computed number of partitions; each index i is mapped to UUID.nameUUIDFromBytes("item-i"). They appear random and unique (checked by distinct().count()) but they are deterministic - if Spark recalculates a lost partition, it will produce the same items that it did previously. The RDD is persisted because it's used to insert and look up. All 10 partitions have just 100,000 records (≤ R), as the script prints their sizes.
Hashing. The k positions in the following hashing scheme are g_i(x) = (h1 + i·h2) mod n, for each i in 0..k-1, where h1 and h2 are two MurmurHash3 hashcodes (the seeds are 17 and 31). This is similar to k independent hash functions, but requires just 2 hash operations for each item.
Task 2 – Distributed insertion. The bit array is an Array [Long] (64 bits per word). The items in every partition are not shuffled across the partitions in mapPartitions, each partition fills their own local bit vector from their own items. Then, using treeReduce(depth 2), the partial vectors are combined using bitwise OR. The network only crosses n-bit vectors and OR is associative and commutative, that is, the result is the same as if the insertions were sequential. The final vector is broadcast to all the executors
Task 3 – Lookup. The only thing mightContain(x) returns false is when at least one of the k bits is 0, which means that the bit is definitely missing from the set; if all k bits are 1, then the bit is present, but not necessarily, and mightContain(x) returns true. Single lookups are done on the driver, batch lookups are done on the executors using rdd.filter on the broadcast vector. Members are a random sample of the data; non-members are created with a different prefix ("absent-i"), and thus can't be in the set.
3. Results

 <img width="789" height="129" alt="image" src="https://github.com/user-attachments/assets/7c12e4bb-7ad4-436d-9847-4cc8f2a12fe7" />

The FPR was measured on 100,000 items that were never inserted. The observed 0.984% is below the 1% target and within one standard deviation of the theory (σ = √(p(1−p)/100,000) ≈ 0.031%). A Bloom filter guarantees there will be no false negatives. The fill ratio of 0.518 is very close to the optimum fill ratio of 0.5.
The script (lines beginning with [time]) prints timing and partitioning statistics for dataset generation, insertion, broadcast and for each look up batch, the look up throughput, and the number of records in each partition. Measured values from my run (spark-shell, local[*]):

<img width="781" height="242" alt="image" src="https://github.com/user-attachments/assets/2d572ebf-68e3-4a38-81d7-2851dcf72876" />

4. Overflow handling: Scalable Bloom Filter
A Bloom filter can't just be increased in size - each bit position is based on n, and if n were increased, then all the original items would have to be re-hashed. As more items are inserted into the fixed filter, it becomes full (fill ratio 0.839) and the FPR increases to 29.4%. I therefore decided to use a Scalable Bloom Filter (Almeida et al., 2007):
•	Layer 0 is the original filter. It is never rebuilt; the script verifies that its number of 1-bits is unchanged after the overflow (no reindexing).
•	New items first fill the remaining capacity of the active (last) layer. When it is full, a new layer i is added with capacity m_i = m·2^i and error p_i = p·0.5^i; its n and k come from the same formulas.
•	Only the new items are hashed, using the same mapPartitions + treeReduce(OR) procedure. zipWithIndex splits a batch into the ranges that fit each layer.
•	A lookup returns true if any layer returns true. There are still no false negatives, because every item is stored in one layer.
FPR_total = 1 − ∏ (1 − p_i)  ≤  Σ p·0.5^i  =  2p
In the run the 1.5 M extra items went into layer 1 (capacity 2,000,000, p_1 = 0.005, n = 22,055,507 bits, k = 8). The total FPR was 1.071%, against 29.4% for the overloaded fixed filter. The two-layer bound is 1 − (0.99)(0.995) = 1.495%, and the long-run bound is 2p = 2%, however many layers are added. Because capacities double, the number of layers grows only logarithmically, so lookups stay fast. A simpler chain of equal filters (capacity m, error p each) would need 3 filters for 2.5·m items and give about 2.0% FPR, and its error keeps growing with every filter. In a second Spark run with 10·m items (9,000,000 extra), the filter grew automatically to 4 layers (capacities m, 2m, 4m, 8m) in 7.3 s; the observed FPR was 1.73% (theory 1.75%), still below the 2% bound, with no false negatives and layer 0 unchanged, while the fixed filter reached 99.5% FPR (fill ratio 0.999).
5. How to run and limitations
Then, run the program in spark-shell: :load BloomFilterSpark.scala and answer the following 5 questions or leave them blank, which will be assumed as 1000000, 0.01, 100000, 150000, 100000 respectively. The partitions that do not fit in memory are spilling to disk for large overflow tests, start the shell with --driver-memory 4g for those tests; cached RDDs will use MEMORY_AND_DISK.
•	Standard Bloom filters don't have a delete operation: a counting Bloom filter would be necessary.
•	Each layer is stored in memory as one broadcast vector (1.1 MB and 2.6 MB for the two layers shown here). The bit array could alternatively be stored as an RDD of blocks keyed by block id for very large n.
•	For global FPR, the value of p should never be exceeded, which means that 0 can be implemented using p·(1−r) = p/2 instead of p, with approximately 15% more bits.


