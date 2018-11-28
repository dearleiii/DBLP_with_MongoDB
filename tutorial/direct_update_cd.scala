/*

this direct update on the original collection without creating new one

*/


object MongoSpark {
    def main(args: Array[String]) {
        /* Uncomment to turn off Spark logs */
        //Logger.getLogger("org").setLevel(Level.OFF)
        //Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("MongoSpark")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)

        val article_input_conf = new Configuration()
        // article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Article")
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/admin.articles")
        article_input_conf.set("mongo.splitter.class","com.mongodb.hadoop.splitter.StandaloneMongoSplitter")

        val inproceedings_input_conf = new Configuration()
        // inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Inproceedings")
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/output.testoutput")
        inproceedings_input_conf.set("mongo.splitter.class","com.mongodb.hadoop.splitter.StandaloneMongoSplitter")

        val article = sc.newAPIHadoopRDD(
            article_input_conf,         // config
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )

        val inproceedings = sc.newAPIHadoopRDD(
            inproceedings_input_conf,
            classOf[MongoInputFormat],
            classOf[Object],
            classOf[BSONObject]
        )

        // Create a separate Configuration for saving data back to MongoDB 
        val outputConfig = new Configuration()
        outputConfig.set("mongo.output.uri", 
            "mongodb://localhost:27017/output.testoutput")

        // update by firstly mapping the operation across the current RDD
        val updates = inproceedings.mapValues(
            value => new MongoUpdateWritable(
                new BasicBSONObject("_id", value.get("_id")),  // Query
                new BasicBSONObject("$set", new BasicBSONObject("foo", "bar")),  // Update operation
                false,  // Upsert
                false,   // Update multiple documents
                false //
            )
        )

        // now saveAsnew APIHadoopFile, using MongoUpdateWritable as the value class 
        updates.saveAsNewAPIHadoopFile(
            "file://this-is-completely-unused", 
            classOf[Object],
            classOf[MongoUpdateWritable],
            classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
            outputConfig)
