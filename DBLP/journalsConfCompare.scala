/* Q3d.
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas). */
    def q3d(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        // article.take(15).foreach(println)

        val journalRDD = article
        .values
        .map(document => document.get("authorList").toString)
        .flatMap(authors => authors.split(",|\\[|\\]"))
        .filter(name => name != "")

        // println("checking journal authors: ")
        // journalRDD.take(20).foreach(println)

        val countJRdd = journalRDD
        .filter(name => name != " ")
        .map{ case (name) => (name, 1) }
        .reduceByKey(_+_)
        .sortBy(_._2, false)

        // println("checking journal authors count: ")
        // countJRdd.take(20).foreach(println)

        val conferenceRDD = inproceedings
        .values
        .map(document => document.get("authorList").toString)
        .flatMap(authors => authors.split(",|\\[|\\]"))
        .filter(name => name != "")
        .filter(name => name != " ")
        .map{ case (name) => (name, 1) }
        .reduceByKey(_+_)
        .sortBy(_._2, false)

        // println("checking conference authors: ")
        // conferenceRDD.take(20).foreach(println)
        
        // Compare: 
        val compareRDD = countJRdd.join(conferenceRDD)
        .map{ case (name, (journal, conference)) => (name, journal, conference) }
        .filter{ case (name, journal, conference) => journal > conference }
        
        println("Find the number of authors who wrote more jounal papers than conference papers: ")
        // compareRDD.take(20).foreach(println)
        println(compareRDD.count)
    }
