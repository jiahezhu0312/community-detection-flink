package wcc

import org.apache.flink.api.scala._
import org.apache.flink.graph.Vertex
/**
 * Adaptation of VertexData class in idwcc spark.
 *
 * @param vId the vertex Identifier.
 * @param t the number of links between neighbors of the vertex aka triangle count.
 * @param vt The number of vertices that form at least one triangle with x.
 */

class VertexData(val vId: Long = -1L,  val t: Int = 0, val vt: Int = 0) extends  Serializable {
  var cId: Long = vId

  def cc: Double = {
    if (vt < 2) {
      0.0
    } else {
      2.0 * t / (vt * (vt - 1))
    }
  }

  var changed: Boolean = false

  var neighbors: List[VertexMessage] = List.empty

  def copy() = {
    val v = new VertexData(this.vId, this.t, this.vt)
    v.cId = this.cId
    v.changed = this.changed
    v.neighbors = this.neighbors
    v
  }

  def isCenter() = {
    this.vId == this.cId
  }

  def compareTo(vs: VertexData) = {
    if (VertexData.ordering.lt(this, vs)) {
      (this, vs)
    } else {
      (vs, this)
    }
  }

  def toTuple() = {
//    (this.vId, this.t, this.vt, this.cc, this.cId, this.changed, this.neighbors.map(x=>x.vId))
    (this.vId, this.cId)

  }

}

object VertexData {
  implicit val ordering: Ordering[VertexData] = Ordering.by({ data: VertexData =>
    (data.cc, data.vt, data.vId)
  })
}