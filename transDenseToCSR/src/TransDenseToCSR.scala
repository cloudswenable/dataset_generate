/**
  * Created by jimmy on 16-5-6.
  */
import java.io.{File, PrintWriter}

import org.apache.spark._
import scopt.OptionParser

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.Sorting

object TransDenseToCSR {
  case class Rating (
                    uid: Int,
                    pid: Int,
                    rating: Double
                    )
  private case class Params (
                              slices:  Int = 1,
                              ratingFile: String = null
                            )

  def run (params: Params) {
    val conf = new SparkConf().setAppName("DataProcess")
    val sc = new SparkContext(conf)
    val ratings = sc.textFile(params.ratingFile).map (line => {
      val fields = line.split(",")
      Rating(fields(1).toInt, fields(0).toInt, fields(2).toDouble)
    })

    val maxUid = ratings.map(_.uid).distinct().max()
    val uidLen = ratings.map(_.uid).distinct().count()
    val ids = ratings.sortBy(_.uid).map(_.uid).distinct().collect()
    var idMaps = new HashMap[Int, Int]()
    //print("max uid xxxxxxxxxxxxxxxxxxx " + maxUid)
    //print("ids xxxxxxxxxxxxxxxxxx " + ids)
    //print("uidLen xxxxxxxxxxxxx  " + uidLen)
    if(maxUid > uidLen) {
      //map uid
      val idflags = new Array[Boolean](uidLen.toInt)
      var i = 1
      for (idx <- ids) {
        idMaps += (idx -> i)
        i += 1
      }
    }

    //print("id map len xxxxxxxxxxxxxxxx " + idMaps.size)
    /*
    idMaps.foreach( e => {
      val (x, y) = e
      print("xxxxxxxxxxxxxxxxxxx "+ x + "  "+ y)
    })
    */
    val ordering = new Ordering[Rating] {
      def compare(a:Rating, b:Rating): Int = {
        val key1 = a.uid - b.uid
        if (key1 == 0) {
          return (a.pid - b.pid)
        } else {
          return key1
        }
      }
    }

    val newUserRatings = ratings.map( r => {
      Rating(idMaps.getOrElse(r.uid, r.uid), r.pid, r.rating)
    }).sortBy(_.uid).cache()
    val newPRatings = newUserRatings.sortBy(_.pid).map( r => {
      Rating(r.pid, r.uid, r.rating)
    }).cache()

    val userBlocks = newUserRatings.map( r => {
      (r.uid % params.slices, r)
    })

    val productBlocs = newPRatings.map( r=> {
      (r.uid % params.slices, r)
    })
/*
    userBlocks.partitionBy(new HashPartitioner(params.slices)).saveAsTextFile("/home/jimmy/user")
    productBlocs.partitionBy(new HashPartitioner(params.slices)).saveAsTextFile("/home/jimmy/product")
*/

    userBlocks.partitionBy(new HashPartitioner(params.slices)).mapPartitionsWithIndex( (id, iters) => {
      val temp_r = new ArrayBuffer[Rating]()
      val writer = new PrintWriter(new File("/home/jimmy/data_process/orignal/user-orig-"+id))
      while(iters.hasNext) {
        if(temp_r.size == 0) {
          temp_r += iters.next()._2
        } else {
          val r = iters.next()._2
          val br = temp_r(0)
          if (r.uid != br.uid) {
            //write to files
            //trunck the buffer
            //add new to buffer
            for (tr <- temp_r) {
              writer.write(tr.uid+","+tr.pid+","+tr.rating+"\n")
            }
            temp_r.clear()
            temp_r += r
          } else {
            var tr = 0
            temp_r.takeWhile( _.pid <= r.pid).foreach(x => tr+=1)
            if (tr == temp_r.size) {
              temp_r.append(r)
            } else {
              temp_r.insert(tr, r)
            }
          }
        }
        //writer.write(r.uid+","+r.pid+","+r.rating+"\n")
      }
      for (r <- temp_r)
        writer.write(r.uid+","+r.pid+","+r.rating+"\n")
      /*
      val sort_r = temp_r.toArray
      Sorting.quickSort(sort_r)(ordering)
      for (r <- sort_r) {
        writer.write(r.uid+","+r.pid+","+r.rating+"\n")
      }
      */
      writer.close()
      Iterator.empty
    }).count()
    productBlocs.partitionBy(new HashPartitioner(params.slices)).mapPartitionsWithIndex( (id, iters) => {
      val temp_r = new ArrayBuffer[Rating]()
      val writer = new PrintWriter(new File("/home/jimmy/data_process/orignal/product-orig-"+id))
      while(iters.hasNext) {
        if(temp_r.size == 0) {
          temp_r += iters.next()._2
        } else {
          val r = iters.next()._2
          val br = temp_r(0)
          if (r.uid != br.uid) {
            //write to files
            //trunck the buffer
            //add new to buffer
            for (tr <- temp_r) {
              writer.write(tr.uid+","+tr.pid+","+tr.rating+"\n")
            }
            temp_r.clear()
            temp_r += r
          } else {
            var tr = 0
            temp_r.takeWhile(_.pid <= r.pid).foreach(x => tr+=1)
            if (tr == temp_r.size) {
              temp_r.append(r)
            } else {
              temp_r.insert(tr, r)
            }
          }
        }
        //writer.write(r.uid+","+r.pid+","+r.rating+"\n")
      }
      for (r <- temp_r)
        writer.write(r.uid+","+r.pid+","+r.rating+"\n")
      /*
      val sort_r = temp_r.toArray
      Sorting.quickSort(sort_r)(ordering)
      for (r <- sort_r) {
        writer.write(r.uid+","+r.pid+","+r.rating+"\n")
      }
      */
      writer.close()
      Iterator.empty
    }).count()
    }

  def main(args: Array[String]) {
    val alsParams = Params()
    val parser = new OptionParser[Params]("DataProcess") {
      head("data process: transfer dense to csr data  for daal")
      opt[Int]("slices")
        .text(s"slices: ${alsParams.slices}")
        .action((x, c) => c.copy(slices= x))
      opt[String]("ratingFile")
        .text(s"rating file for training: ${alsParams.ratingFile}")
        .action((x, c) => c.copy(ratingFile = x))
    }

    parser.parse(args, alsParams).map {
      params => run(params)
    }.getOrElse {
      parser.showUsageAsError
      System.exit(1)
    }
  }

}
