package hu.elte.inf.sampling.core

import java.nio.ByteBuffer

object Shop {
  type Type = Long

  def fromByteBuffer(bb: ByteBuffer): Type = {
    bb.getLong
  }

  def fromInt(i: Int): Type = {
    i.toLong
  }
}