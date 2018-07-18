import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTest{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val a = sc.parallelize(1 to 9)
    a.flatMap(line => {
      val t = line*3
      val q = line*10
      List(t,q)
    }).foreachPartition(iter =>{
      while (iter.hasNext){
        println(iter.next())
      }
    })

  }

}