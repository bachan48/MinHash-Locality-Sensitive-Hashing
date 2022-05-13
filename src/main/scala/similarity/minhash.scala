package similarity

import org.apache.spark.rdd.RDD

object minhash {

/* Student Name: Bachan Ghimire
   Student ID: 00996378
   Program: Level 500, MSc
 */

  def shingle_line(stList: List[String], shingleSize: Int): Shingled_Record = {
    if(stList==null || stList.head == "") throw new RuntimeException("One or more lists of token is empty.")
    val id = stList.head
    val shinglesString = stList.tail.sliding(shingleSize).toSet
    val hashedShingles = shinglesString.map(x=>utils.hash_string_lst(x))
    val shinglesHashResult = if(stList.tail.size >=  shingleSize) Option(hashedShingles) else None
    Shingled_Record(id, hashedShingles.size, shinglesHashResult)
  }

  def compute_jaccard_pair(a: Shingled_Record, b: Shingled_Record): Similarity = {
    Similarity(a.id, b.id, (((a.shingles.get).intersect(b.shingles.get)).size.toDouble / ((a.shingles.get).union(b.shingles.get)).size.toDouble))
  }

  def find_jaccard_matches(records: RDD[Shingled_Record], minSimilarity: Double): Matches = {
    val allPairs = records.cartesian(records).filter(r=>r._1.id < r._2.id) //keep idA < idB
    val matches = allPairs.map(pair=>compute_jaccard_pair(pair._1, pair._2)).filter(record=>record.sim >= minSimilarity).collect()
    matches
  }

  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = {
    val minHashes=  hashFuncs.map(function => {
      val hashes = r.shingles.get.map(shingle=>
        function(shingle))
      hashes.min //keep only the minimum hashes for a shingle form all functions provided
    }).toVector
    Min_Hash_Record(r.id, minHashes)
  }

  def find_minhash_matches(records: RDD[Min_Hash_Record], minSimilarity: Double): Matches = {
    val allPairs = records.cartesian(records).filter(r=>r._1.id < r._2.id) //keep idA < idB
    val matches = allPairs.map(pair=>{
      val sim = pair._1.minHashes.intersect(pair._2.minHashes).size.toDouble / pair._1.minHashes.size.toDouble
      Similarity(pair._1.id, pair._2.id, sim)
    }).filter(record=>record.sim>=minSimilarity).collect()
    matches
  }

  def find_lsh_matches(records: RDD[Min_Hash_Record], minSimilarity: Double, bandSize: Int): Matches = {
    val bucketsWithSimilarRecords = records.map(record=>{
      val buckets = record.minHashes.grouped(bandSize)
      val bucketHash = buckets.map(bucket=>{
        utils.hash_int_lst(bucket.toList)
      })
      Min_Hash_Record(record.id, bucketHash.toVector)
    }).persist()

    val allPairs = bucketsWithSimilarRecords.cartesian(bucketsWithSimilarRecords).filter(r=>r._1.id < r._2.id) //keep idA < idB
    val matches = allPairs.map(pair=>{
      val sim = pair._1.minHashes.intersect(pair._2.minHashes).size.toDouble / pair._1.minHashes.size.toDouble
      Similarity(pair._1.id, pair._2.id, sim)
    }).filter(record=>record.sim>=minSimilarity).collect()

    matches
  }

}