val cheminpath ="C:/Users/ervan/Documents/GitHub/PR7BIGDATA/getData/storage"
val datajour =spark.read.option("inferSchema" ,true).option("header" , true).option("multiline","true").option("mode","DROPMALFORMED").json(s"$cheminpath/Datajours4.json")

datajour.printSchema()

datajour.select("city.country").show()

// afficher les dates

 val datajour =spark.read.option("inferSchema" ,true)
   .option("header" , true)
   .option("multiline","true")
   .option("mode","DROPMALFORMED")
   .json(s"$cheminpath/Data15jours4.json")

   Selection du champs "list" et ses sous champs
    val dateDF= datajour.select(explode(col("list")) .as("list_element")).select("list_element.dt_txt")

    dateDF.show(false)


    //afficher la temperature minimale 
     val tempDF = datajour.select(explode(col("list")).as("list_element")).select("list_element.main.temp_min")

    //convertir en K  en C°
    val temperature = datajour.select(explode(col("list")).as("list_element")) .select(expr("CAST(list_element.main.temp_min AS DOUBLE) - 273.15").as("celsius"))

    //jointure des deux tables 
    // Ajout d'une colonne d'index pour la jointure
val dateDFIndex = dateDF.withColumn("index", monotonically_increasing_id())
val temperatureIndex = temperature.withColumn("index", monotonically_increasing_id())

// Jointure sur la colonne "index"
val temp_dateJoin = dateDFIndex.join(temperatureIndex, Seq("index"), "inner").drop("index")

temp_dateJoin.show(40)

//du jour le plus froid au jour le moins froid 

val dateFroid = temp_dateJoin.orderBy(asc("celsius")).select("dt_txt", "celsius")
 dateFroid.show(40)

 //le jour le plus froid uniquement 
 val jourleplusfroid = temp_dateJoin.orderBy("celsius").select("dt_txt", "celsius").first()