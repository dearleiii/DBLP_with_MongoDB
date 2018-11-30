    /* Q3b.
     * Find the TOP­20 authors who published the most number of papers in
     * "Database" (author published in multiple areas will be counted in all those
     * areas). */
    
    def q3b(inproceedings: RDD[(Object,BSONObject)]) { 
        // RDD: Object: key string, BSONObject: all information 
        val databaseRDD = inproceedings
        .values
        .filter( document => (document.get("Area").toString) == "Database")

        val authorsRDD  = databaseRDD
        .map(document => document.get("authorList").toString)
        .flatMap(authors => authors.split(",|\\[|\\]"))  // author type: object 
        .filter(name => name != "")// case org.apache.spark.rdd.RDD[Any]->[String]

        val countRDD = authorsRDD
        .map{ case (name) => (name, 1) }
        .reduceByKey(_+_)
        .sortBy(_._2, false)

        countRDD.take(20).foreach(println)
    }
