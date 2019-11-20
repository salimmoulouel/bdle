//http://www-bd.lip6.fr/wiki/_media/site/enseignement/master/bdle/ratt_bdle_sep2018.pdf
//q1
val tuples = data.map(x => x.split("::")).map(x=> ((x(0),x(1)),x(2))).flatMapValues(x => x.split("\\|")).map(x => (x._1._1,x._1._2,x._2))

val compteParGenre = tuples.map(x=> (x._3,1)).reduceByKey(_+_)


//q2
val res = triples.filter("prop='p1'").as("t1").join(triples.as("t2"),$"t2.sujet"===$"t1.objet").select($"t1.sujet",$"t2.objet").as("t").join(triples.as("t3"),$"t3.sujet"===$"t.objet").select($"t.sujet",$"t.objet").distinct()


//q3


val r1 = triples.filter("prop='p4'").withColumnRenamed("sujet","x1").withColumnRenamed("objet","y")
val r2 = triples.filter("prop='p4'").withColumnRenamed("sujet","x2").withColumnRenamed("objet","y")
val r3 = triples.filter("prop='p5'").withColumnRenamed("sujet","y").withColumnRenamed("objet","w1")

val r4 = triples.filter("prop='p5'").withColumnRenamed("sujet","y").withColumnRenamed("objet","w2")
val r5 = triples.filter("prop='p6'").withColumnRenamed("sujet","w1").withColumnRenamed("objet","w22")


val res = r1.join(r2,"y").join(r3,"y").join(r4,"y").join(r5,"w1").filter("x1>x2").select("x1","x2").distinct	




