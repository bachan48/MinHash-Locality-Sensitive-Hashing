package object similarity {


  case class Shingled_Record (
    id: String,
    nWords: Int,
    shingles: Option[Set[Int]],
  )

  case class Min_Hash_Record (
    id: String,
    minHashes: Vector[Int],
  )

  case class Similarity (
    idA: String,
    idB: String,
    sim: Double
  )

  type Matches = Array[Similarity]

  type Hash_Func = Int => Int
}

package similarity {

  import java.util.zip.CRC32
  import scala.annotation.tailrec
  import scala.util.Random

  object utils {

    // use 4 bytes for shingles (Int... actually, 31 bits due to sign)

    val PRIME =  4294967311L // must be next prime larger than MAX_SHINGLE_ID

    // seed the  random number generator, so we have reproducible results
    private val rand = new Random(PRIME)

    private val MAX_SHINGLE_ID = (1L << 32) -1

    private val SEP = ";"

    private def crc32(st:String):Int = {
      val crc=new CRC32
      crc.update(st.getBytes)
        (crc.getValue & MAX_SHINGLE_ID).toInt
    }

    def hash_string_lst(lst:List[String]):Int = {
      crc32(lst.mkString(SEP))
    }

    def hash_int_lst(lst:List[Int]) = {
      crc32(lst.toString)
    }

    // Generate hashing functions. We need HASH_COUNT different ones

    @tailrec
    private def k_random(k:Int, max:Long) : List[Long] = {
      // return a vector of k random distinct integers
      val lst = for(i<- 1 to k) yield rand.nextLong()
      val l2 = lst.toSet
      val l3 = lst.toList
      // make sure it has exactly k elements
      // simply recurse until we get exactly k different ones
      // it has to happen at some point :)
      if (l2.size == k)
        l3
      else
        k_random(k, max)
    }

    def generate_random_coefficients(k: Int) : List[Long] = {
      // generate k random coeffients without replacement
      k_random(k, utils.MAX_SHINGLE_ID)
    }

    def create_hash_functions(aCoefs: List[Long], bCoefs: List[Long]): List[Hash_Func] = {
      def hash_function(a:Long,b:Long)(shingle:Int) = ((a * shingle + b) % utils.PRIME).toInt

      aCoefs.zip(bCoefs).map{
        case (a,b) =>
          hash_function(a,b)(_)
      }
    }

    def do_report(jaccardResults: Matches, minHashesResults: Matches, lshResults: Matches) = {
      // get set of all recordids
      def toSet (recs: List[Similarity]) = {
        recs.map(r => r.idA + "," + r.idB).toSet
      }

      def precision(v: Double):Double = {
        val digits = 5
        Math.floor(v * Math.pow(10,digits))/Math.pow(10, digits)
      }

      def double_to_string(v: Double) =
        "%6.5f".format(precision(v))

      def matches_to_map (recs: Matches):Map[String, String] = {
        recs.map(r => (r.idA + "," + r.idB, double_to_string(r.sim))).toMap.withDefaultValue("---")
      }

      val jac = matches_to_map(jaccardResults)

      val minhash = matches_to_map(minHashesResults)
      val lsh = matches_to_map(lshResults)
      val ids = (jac.keys.toSet).union(minhash.keys.toSet).union(lsh.keys.toSet).toList

      val maxWidth = if (ids.size > 0)  ids.map(_.size).max else 10

      val w = "%20s"
      val header = s"%-${maxWidth}s ${w} ${w} ${w}".format("Pair", "Jaccard", "minHash", "lsh")
      println(header);
      val sep = "-"*(header.size)
      println(sep)
      ids.sorted.foreach { k =>
        println(s"%-${maxWidth}s ${w} ${w} ${w}".format(k,
          jac(k), minhash(k), lsh(k))
        )
      }
      println(sep)
    }

  }

  


}



