import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

import com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}

import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}

import com.mongodb.hadoop.io.MongoUpdateWritable


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
            inproceedings_inputconf, 
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )
        
        // create a separate Configuration for saving data back to MongoDB 
        val outputConfig = new Configuration()
        outputConfig.set("mongo.output.uri", 
            "mongodb://localhost:27017/output.testoutput")
        // this save RDD inproceedings as a Hadoop "File"
        // result in a new collection in the outputConfig directory 
        inproceedings.saveAsNewAPIHadoopFile(
            "file://this-is-completely-unused", 
            classOf[Object],
            classOf[BSONObject],
            classOf[MongoOutputFormat[Object, BSONObject]],
            outputConfig
        )
        
    }
}
