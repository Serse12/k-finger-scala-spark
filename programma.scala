package it.unisa.di.soa
// scalastyle:off println

import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer



object ProvaClasse {

    val k = 7
    val min_total_length = 40
    val min_shared_kmers = 4
    val max_kmer_occurrence = -1
    val max_diff_region_percentage = 0.0
    val min_region_length = 100
    val min_region_kmer_coverage = 0.27
    val min_overlap_coverage = 0.70
    val min_overlap_length = 600

    var local = true

    var sc:  SparkContext = null
    var outputPath: String = "output"
    var inputPath: String = "input"

    def main(args: Array[String]) {

      if (args.length < 2) {
        System.err.println("Errore nei parametri sulla command line")
        System.err.println("Usage:\nit.unisa.di.soa.WordCount inputDir outputDir [local|masterName]")
        throw new IllegalArgumentException(s"illegal number of argouments. ${args.length} should be at least 2")
        return
      }

      outputPath = args(1)
      inputPath = args(0)

      if (args.length > 2) {
        local = (args(2).compareTo("local") == 0)
      }
      val sparkConf = new SparkConf().
            setAppName("Scala WordCount").
            setMaster(if (local) "local" else "yarn")
      // Create the SparkContext
      val sc = new SparkContext(sparkConf)

      inputPath = if (local) "data/" else s"hdfs://${args(2)}:9000/${inputPath}"

      println(s"Opening Input Dataset: ${inputPath}")
      
      val fingerprintlist = sc.textFile(inputPath)
        .map(line => getreads(line))

      val fingerpintlist_onlyint = fingerprintlist.map(getfingerpintlistonlyint)
        
      val result = fingerprintlist.flatMap(triple => getkmers(triple))
        .reduceByKey(_++_)
        .flatMap(x => getmatchesdict(x))
       .reduceByKey((a,b) =>{
          if(a(0) > b(0)){
            a(0)=b(0)
            a(1)=b(1)
          }
          if(a(2) < b(2)){
            a(2)=b(2)
            a(3)=b(3)
          }

          if (a(4) < min_shared_kmers) {a(4) = a(4)+b(4)}

          a

        })
        .filter((tupla) => tupla._2(4) >= min_shared_kmers)
        .map(y => (y._1,(y._2(0),y._2(1),y._2(2),y._2(3),y._2(4))))


      printf("\nFINITO\n")
      result.saveAsTextFile(outputPath)
        
      sc.stop()
    }
    


    def getreads(line:String): Array[(Int,Int,Int)] ={

      val l_line = line.split(" ").filter(_ != "|")
      val read_id=l_line.head.split("_")
      val fingerprint_list=l_line.slice(2,l_line.size)
      
     
      val res_fin_list = new ArrayBuffer[(Int,Int,Int)]()
      
      var temp = 1
      if(read_id(1)=="1") temp = 0

      for(i <- 0 until fingerprint_list.size by 1){
        res_fin_list +=((fingerprint_list(i).toInt,read_id(0).toInt*2+temp,i))
      }
      return res_fin_list.toArray

    }

    def getkmers(triple: Array[(Int,Int,Int)]): Array[(String,ArrayBuffer[(Int,Int)])]={
      
      var res = ArrayBuffer[(String,ArrayBuffer[(Int,Int)])]()

      for (i <- 0 until triple.size-k by 1){
        var somma=0
        var kmer = ""
        for(j <- 0 until k by 1){
          var temp_v =triple(i+j)._1
          somma=somma+temp_v
          kmer+=temp_v.toString+"_"
        }
        if (somma >= min_total_length)
          res += ((kmer,ArrayBuffer((triple(i)._2,triple(i)._3))))
       }

       return res.toArray

    }

    
    def getmatchesdict(t_entry : (String,ArrayBuffer[(Int,Int)])):Array[(String,Array[Int])] ={

      val matchesdict = new ArrayBuffer[(String, Array[Int])]()

      for (i <- 0 until t_entry._2.size by 1){
        var first_occ = t_entry._2(i)._1

        for (j <- i+1 until t_entry._2.size by 1){
          var second_occ = t_entry._2(j)._1
          var key = first_occ+"_"+second_occ
          var value = new Array[Int](5)

          value(0)=t_entry._2(i)._2
          value(1)=t_entry._2(j)._2
          value(2)=t_entry._2(i)._2
          value(3)=t_entry._2(j)._2

          value(4) = 1
          matchesdict += ((key,value))
        }

      }

      return matchesdict.toArray


    }

    def getfingerpintlistonlyint(entry : Array[(Int,Int,Int)]):Array[Int]={

      var sum=0

      val res_fin_list = new ArrayBuffer[Int]()

      for(i <- 0 until entry.size by 1){
        res_fin_list +=(entry(i)._1)
        sum += entry(i)._1
      }

      res_fin_list += sum

      return res_fin_list.toArray



    }




}


