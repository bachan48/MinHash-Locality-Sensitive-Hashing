package similarity

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  // Reduce level of messages from Spark while running

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on

  // process arguments

  if (args.size != 9) {
    val sep = " "
    System.err.println(s"Arguments missing <filename> <delimiter> <minimumSimilarity> <shingleSize> <hashCount> <bandSize> <doJaccard> <doAllMinHashes> <printHashCoefficients> <outputHashFunctions>. Only provided ${args.size} parameters\n   Provided ${args.mkString(sep)}")
    throw new IllegalArgumentException(s"Program terminated...")

  }
  val filename = args(0)
  val SEPARATOR = args(1)
  val MIN_SIM = args(2).toDouble
  val SHINGLE_SIZE = args(3).toInt
  val HASH_COUNT = args(4).toInt
  val BAND_SIZE = args(5).toInt
  val DO_JACCARD = args(6).toBoolean
  val DO_ALL_MIN_HASHES = args(7).toBoolean
  val PRINT_HASH_COEFS = args(8).toBoolean

  println("Computing similarity with parameters: ")
  List(("Filename", filename), ("Separator", "[" + SEPARATOR + "]"), ("Minimum similarity", MIN_SIM),
    ("Shingle size", SHINGLE_SIZE), ("Hash count", HASH_COUNT), ("Band size", BAND_SIZE),
    ("Compute Jaccard similarity", DO_JACCARD),
    ("Compute all minHashes similarity", DO_ALL_MIN_HASHES),
    ("Print hash coefficients", PRINT_HASH_COEFS),
  ).map {
    case (st, v) => println(s"    ${st}: $v")
  }
  println()

  //-------------------------------
  // starting...

  val lines = sc.textFile(filename)

  // create hash functions needed
  val aHashCoefs = utils.generate_random_coefficients(HASH_COUNT)
  val bHashCoefs = utils.generate_random_coefficients(HASH_COUNT)

  if (PRINT_HASH_COEFS) {
    println("Hash coefficients:")
    println(aHashCoefs)
    println(bHashCoefs)
  }

  def hashFunctions: List[Hash_Func] = utils.create_hash_functions(aHashCoefs, bHashCoefs)

  System.err.println("Shingling records...")

  val docs = lines.
    filter(_.contains(SEPARATOR)).
    map { line =>
      val tokens = line.split(SEPARATOR).filter(_ != "").toList
      minhash.shingle_line(tokens, SHINGLE_SIZE)
    }.
    filter(_.shingles.isDefined).persist

  System.err.println("Minhashing records...")

  val minHashes = docs.
    map(r => minhash.minhash_record(r, hashFunctions)).persist

  //do lhs now
  val jac: Matches =
    if (DO_JACCARD) {
      System.err.println("Doing Jaccard comparison... this might be slow...")
      minhash.find_jaccard_matches(docs, MIN_SIM)
    } else
      Array()

  val minHashesMatches: Matches =
    if (DO_ALL_MIN_HASHES) {
      System.err.println("Doing all min hashes comparison...")
      minhash.find_minhash_matches(minHashes, MIN_SIM)
    } else Array()

  System.err.println("Doing LHSs comparisons...")

  val lshs = minhash.find_lsh_matches(minHashes, MIN_SIM, BAND_SIZE)

  System.err.println("Doing report.")
  utils.do_report(jac, minHashesMatches, lshs)
  System.err.println("Finished.")

  sc.stop()

}
