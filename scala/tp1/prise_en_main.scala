//Question 1

val listeEntiers = List.range(1,11)

def maxEntiers(liste:List[Int])=liste.max

def scEntiers(liste:List[Int])=liste.map(x=>x*x).reduce((a,b)=>a+b)

def moyEntiers(liste:List[Int])=(liste.reduce((a,b)=>a+b))/liste.length

//Question 2

val listeTemp = List("7,2010,04,27,75", "12,2009,01,31,78", "41,2009,03,25,95", "2,2008,04,28,76", "7,2010,02,32,91")

val temp2009 = listeTemp.map(x=>x.split(",")).filter(x=>x(1)=="2009").map(x=>x(3).toInt) 

println("max des temps " + maxEntiers(temp2009))

println("moy des temps " + moyEntiers(temp2009))


//Question 3

val melange = List("1233,100,3,20171010", "1224,22,4,20171009", "100,lala,comedie", "22,loup,documentaire")

val notes = melange.map(x=>x.split(",")).filter(x=>x.length==4).map(x=>(x(0).toInt, x(1).toString, x(2).toInt, x(3).toInt))


val films = melange.map(x=>x.split(",")).filter(x=>x.length==3).map(x=>(x(0).toInt, x(1).toString, x(2).toString))

//Question 4

val personnes = List(("Joe", "etu", 3), ("Lee", "etu", 4), ("Sara", "ens", 10), ("John", "ens", 5), ("Bill", "nan",20))

case class Etu(nom:String, annee:Int)

case class Ens(nom:String, annee:Int)

val possible=List("etu","ens")
personnes.filter(x=>(possible.contains(x._2))).map(x=> x._2 match {
case "etu"=>Etu(x._2,x._3)
case "ens"=>Ens(x._2,x._3)})
//case _=>"delete"}).filter(x=>x!="delete")

