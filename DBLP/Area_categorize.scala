def q2(inproceedings: RDD[(Object,BSONObject)]) {
        val outputConfig = new Configuration()
        outputConfig.set("mongo.output.uri", 
            "mongodb://localhost:27017/output.testoutput2")

        // update by firstly mapping the operation across the current RDD
        val updates = inproceedings.mapValues(
            value => {
                val name_conf : String = value.get("booktitle").toString
                if ((name_conf == "SIGMOD Conference") | (name_conf == "VLDB") |
                (name_conf == "ICDE") | (name_conf == "PODS"))
                {
                    new MongoUpdateWritable(
                        new BasicBSONObject("_id", value.get("_id")),  // Query
                        new BasicBSONObject("$set", new BasicBSONObject("Area", "Database")),  // Update operation
                        false, false, false)
                } else if ((name_conf == "STOC") | (name_conf == "FOCS") |
                (name_conf == "SODA") | (name_conf == "ICALP"))
                {
                    new MongoUpdateWritable(
                        new BasicBSONObject("_id", value.get("_id")),  // Query
                        new BasicBSONObject("$set", new BasicBSONObject("Area", "Theory")),  // Update operation
                        false, false, false)
                } else if ((name_conf == "SIGCOMM") | (name_conf == "ISCA") |
                (name_conf == "HPCA") | (name_conf == "PLDI"))
                {
                    new MongoUpdateWritable(
                        new BasicBSONObject("_id", value.get("_id")),  // Query
                        new BasicBSONObject("$set", new BasicBSONObject("Area", "Systems")),  // Update operation
                        false, false, false)
                } else if ((name_conf == "ICML") | (name_conf == "NIPS") |
                (name_conf == "AAAI") | (name_conf == "IJCAI"))
                {
                    new MongoUpdateWritable(
                        new BasicBSONObject("_id", value.get("_id")),  // Query
                        new BasicBSONObject("$set", new BasicBSONObject("Area", "ML-AI")),  // Update operation
                        false, false, false)
                } else { 
                    new MongoUpdateWritable(
                        new BasicBSONObject("_id", value.get("_id")),  // Query
                        new BasicBSONObject("$set", new BasicBSONObject("Area", "UNKNOWN")),  // Update operation
                        false, false, false)
                }
            }
        )
        

        // save 
        updates.saveAsNewAPIHadoopFile(
            "file://this-is-completely-unused", 
            classOf[Object],
            classOf[MongoUpdateWritable],
            classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
            outputConfig)
    }
