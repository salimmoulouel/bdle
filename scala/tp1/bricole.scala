def utilite(entre:Array[String]):List[(String,String,String)]={
	val tmp=entre(2).split("\\|")
	var ret=List[(String,String,String)]()
	for (i <- 0 to tmp.length-1) {
		ret=(entre(0),entre(1),tmp(i))::ret
	}
	return ret
}


 data.map(x=>x.split("::")).map(x=>((x(0),x(1)),x(2))).flatMapValues(x=>x.split("\\|"))

val a=data.map(x=>x.split("::"))



val result=a.flatMap(utilite)
