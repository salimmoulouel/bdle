//q1

val q1 = p0.map({case (s,p,o) => (o,s)}).join(p1.map({case (s,p,o)=> (s,o)})).map({ case (y,(x,w))=>(w,x) }).join(p2.map({case (w,p,z)=>(w,z)})).map({case (w,(x,z)) => (x,z)})


//q2

 val q2 = p2.map({ case (x,p,z)=> (x,z)}).join(p1.map({ case (y,p,x)=> (x,y)})).map({case (x,(z,y))=> (x,z)}).join(p3.map({case (x,p,w) => (x,w)})).map({case(x,(z,w))=>(x,z)})
 
 
 //exo 2

//q1

val t1 = StudiedAt.as("t1").join(LocatedAt.as("t2"),$"t1.objet"===$"t2.sujet").select($"t1.sujet",$"t1.objet" as "obj1",$"t2.objet")

val R1 = t1.as("t1").join(SupervisedBy.as("t2"),$"t1.sujet"===$"t2.sujet" && !($"t1.objet"===$"t2.objet")).select($"t1.sujet",$"t1.obj1" as "u",$"t1.objet")


//q2

val t1 = StudiedAt.filter("sujet='luke'")

val t2= StudiedAt.filter("sujet!='luke'")

val R2= t1.as("t1").join(t2.as("t2"),($"t1.objet"===$"t2.objet")).select($"t2.sujet").distinct

//q3

val R3 = 

//q4

val t1 = StudiedAt.as("t1").join(SupervisedBy.as("t2"),$"t1.sujet"===$"t2.sujet").select($"t1.sujet",$"t1.objet" as "u",$"t2.objet")

val t2 = t1.as("t1").join(StudiedAt.as("t2"),$"t1.objet"===$"t2.sujet" && $"t1.u"===$"t2.objet").select($"t1.sujet",$"t1.objet")
