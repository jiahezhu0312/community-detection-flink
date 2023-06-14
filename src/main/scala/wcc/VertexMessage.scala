package wcc

/**
 * Adaptation of VertexMessage class.
 *
 */
class VertexMessage extends Serializable {
  var vId = -1L
  var vt = 0
  var cId= -1L
  var cc = 0.0

  def this(vId: Long, t: Int, vt: Int, cId: Long) {
    this()
    this.vId = vId
    this.cId = cId
    this.vt = vt
    this.cc = if (vt < 2) {
      0.0
    } else {
      2.0 * t / (vt * (vt - 1))
    }
  }

  def isCenter = {
    this.vId == this.cId
  }

}

object VertexMessage {

  def create(vertexData: VertexData) = {
    new VertexMessage(vertexData.vId, vertexData.t, vertexData.vt, vertexData.cId)
  }

  implicit val ordering: Ordering[VertexMessage] = Ordering.by({ data =>
    (data.cc, data.vt, data.vId)
  })
}