package hu.elte.inf.sampling.datagenerator

class ConstantStreamGenerator[TItem](value: TItem)
  extends StreamGenerator[TItem](0) {

  override def next(): TItem = value
}