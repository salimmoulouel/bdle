val path = "/tmp/BDLE/dataset/" 
val data = sc.textFile(path + "wordcount.txt")

//data.take(10)


val list=data.map(x=>x.split(" "))

val q2 = list.map(x=>x(2)) 

val q3 = list.map(x=>(x(0),x(2)))


val q4 = q3.reduceByKey((u,v)=>u+v)

val q5= list.map(x=>(x(0).split("\\.")(0),x(2))).reduceByKey((u,v)=>u+v)




