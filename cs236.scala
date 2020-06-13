//val sc = new SparkContext( "local", "Word Count", "/usr/local/spark", Nil, Map(), Map()) 
		
/* local = master URL; Word Count = application name; */  
/* /usr/local/spark = Spark Home; Nil = jars; Map = environment */ 
/* Map = variables to work nodes */ 
/*creating an inputRDD to read text file (in.txt) through Spark context*/ 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object cs236 {
	def main(args: Array[String]) {

		val t1 = System.nanoTime
		// val folder = readLine("Enter folder path containing WeatherStationLocations.csv, 2006.txt, 2007.txt, 2008.txt and 2009.txt: ")
		if (args.length < 2) {
			System.out.println("Usage: [location file folder] [recordings file folder] [output folder]")
			System.exit(0)
		}
		val lfolder = args(0)
		val rfolder = args(1)
		val dfolder = args(2)
		val spark = SparkSession.builder.appName("cs236").getOrCreate()
		

		// val input = spark.read.option("header", "true").csv(args[0].toString);
		var wsl = spark.sparkContext.textFile(lfolder + "/WeatherStationLocations.csv")
		var data06 = spark.sparkContext.textFile(rfolder + "/200*.txt")

		wsl = wsl.map(s => s.trim.replaceAll("\"", ""))
		var wslrdd = wsl.filter(s => s.contains(",US,") && !s.contains(",US,,"))
		var wslrdd1 = wslrdd.map(s => {
			val fields = s.split(",").toList
			(fields(0), fields(4))
		})
		var wslrdd2 = wslrdd1.sortByKey()

		data06 = data06.map(s => s.trim.replaceAll(" +", " "))
		var data06rdd = data06.filter(s => !s.startsWith("STN---") && !s.contains(" 99.99 "))
		var data06rdd1 = data06rdd.map(s => {
			val fields = s.split(" ").toList
			(fields(0), (fields(2).toInt, fields(19)))
		})

		var join06 = wslrdd2.join(data06rdd1).map {
			case ((a), (b, c)) => (a, b, c._1, c._2)
		}
		val join061 = join06.map(s => {
			if(s._4.contains("A")) {
				val temp = s._4.replaceAll("A", "").toDouble
				(s._1, s._2, s._3, temp*6)
			} else if(s._4.contains("B")) {
				val temp = s._4.replaceAll("B", "").toDouble
				(s._1, s._2, s._3, temp*12)
			} else if(s._4.contains("C")) {
				val temp = s._4.replaceAll("C", "").toDouble
				(s._1, s._2, s._3, temp*18)
			} else if(s._4.contains("D")) {
				val temp = s._4.replaceAll("D", "").toDouble
				(s._1, s._2, s._3, temp*24)
			} else if(s._4.contains("E")) {
				val temp = s._4.replaceAll("E", "").toDouble
				(s._1, s._2, s._3, temp*12)
			} else if(s._4.contains("F")) {
				val temp = s._4.replaceAll("F", "").toDouble
				(s._1, s._2, s._3, temp*24)
			} else if(s._4.contains("G")) {
				val temp = s._4.replaceAll("G", "").toDouble
				(s._1, s._2, s._3, temp*24)
			} else if(s._4.contains("H")) {
				(s._1, s._2, s._3, 0.0)
			} else if(s._4.contains("I")) {
				(s._1, s._2, s._3, 0.0)
			} else {
				(s._1, s._2, s._3, s._4.toDouble)
			}
		})
		// join061.filter(s => s._1 == "727470").take(50).foreach(println)

		val join062 = join061.map(s => (s._1, s._2, s._3/100, s._4))
		// join062.filter(s => s._1 == "727470").take(50).foreach(println)
		val join063 = join062.map(s => {
			((s._2, s._3), (s._4.toDouble, 1))
		})
		val join064 = join063.reduceByKey {
			case ((a, b), (c, d)) => (a + c, b + d)
		}
		val join065 = join064.map(s => ((s._1._1, s._1._2), (s._2._1/s._2._2)))
		val monthlyAvg = join065.sortByKey(true)
		// monthlyAvg.take(100).foreach(println)
		// System.out.println(monthlyAvg.count)

		val high = monthlyAvg.map(s => ((s._1._1), (s._1._2, s._2)))
		val high1 = high.reduceByKey{
			case ((a, b), (c, d)) => {
				if(math.max(b, d) == b) {
					(a, BigDecimal(b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
				} else {
					(c, BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
				}
			}
		}
		val highest = high1.sortByKey(true)
		highest.take(100).foreach(println)
		// System.out.println(highest.count)

		val low = monthlyAvg.map(s => ((s._1._1), (s._1._2, s._2)))
		val low1 = low.reduceByKey{
			case ((a, b), (c, d)) => {
				if(math.min(b, d) == b) {
					(a, BigDecimal(b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
				} else {
					(c, BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
				}
			}
		}
		val lowest = low1.sortByKey(true)
		lowest.take(100).foreach(println)
		// System.out.println(lowest.count)

		var diff = highest.join(lowest).map {
			case ((a), (b, c)) => (a, " highest(date, precipitation): " + b, " lowest(date, precipitation): " + c, " difference: " + BigDecimal(b._2 - c._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
		}
		val result = diff.sortBy(s => s._4, false)
		result.take(100).foreach(println)

		result.coalesce(1).saveAsTextFile(dfolder)

		val duration = Math.round((System.nanoTime - t1) / 1e9d)

		System.out.println("----------------------------------------------------")
		System.out.println("	Job Complete, Runtime: " + duration + "seconds")
		System.out.println("----------------------------------------------------")
		// val country = input.filter("CTRY == 'US' AND STATE != ''")
		// val states = input.select(input("STATE")).distinct.filter("STATE != '' AND CTRY == 'US'")
		// val state = input.sort("STATE").filter("CTRY == 'US' AND STATE != ''")
		// val currState = "CA"
		// val filter = "STATE == '" + currState + "'"
		// val currGroup = state.filter(filter)
		// currGroup.show(5)

		// while(state.isEmpty == false) {

		// }
		//System.out.println(state.count())
		/* saveAsTextFile method is an action that effects on the RDD */  
		// country.write.format(WeatherStationLocations).option("header", "true").csv("outfile/country")
		// state.coalesce(1).write.csv("outfile/state");

		System.exit(0)
	}
}
