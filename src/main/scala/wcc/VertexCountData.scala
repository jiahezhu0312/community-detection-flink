package wcc

class VertexCountData(val vId:Long, val adjList: List[Long] = List.empty[Long], val t: Int = 0){
  val degree = adjList.length


  def toTuple() = {
    (this.vId, this.t, this.degree)

  }
}