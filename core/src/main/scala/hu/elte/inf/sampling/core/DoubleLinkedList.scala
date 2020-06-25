/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Farid Zakaria <farid.m.zakaria@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package hu.elte.inf.sampling.core

import hu.elte.inf.sampling.core.DoubleLinkedList.Node

/**
 * A linked list collection that returns the inner {@link Node} structure as opposed to {@link java.util.LinkedList}
 * which abstracts the inner structure away from you. This can be sometimes useful when you want to do O(1) insert
 * and deletes if you store reference to the {@link Node}
 */
class DoubleLinkedList[T]() extends Iterable[T] {
  /**
   * Pointer to the head of the linked list
   */
  protected var listHead: Node[T] = _

  /**
   * Pointer to the tail of the linked list
   */
  protected var listTail: Node[T] = _

  /**
   * Size of the linkedList
   */
  protected var listSize: Long = 0L


  def getTail: Node[T] = listTail

  def getHead: Node[T] = listHead

  def getSize: Long = listSize


  /**
   * Insert newNode after node
   *
   * @param node
   * @param newNode
   */
  // synchronized
  def insertAfter(node: Node[T], newNode: Node[T]): Unit = this.synchronized {
    newNode.prev = node
    newNode.next = node.next
    if (node.next == null) {
      listTail = newNode
    } else {
      node.next.prev = newNode
    }
    node.next = newNode
    listSize = listSize + 1
  }

  /**
   * Insert newNode before node
   *
   * @param node
   * @param newNode
   */
  def insertBefore(node: Node[T], newNode: Node[T]): Unit = this.synchronized {
    newNode.prev = node.prev
    newNode.next = node
    if (node.prev == null) {
      listHead = newNode
    } else {
      node.prev.next = newNode
    }
    node.prev = newNode
    listSize = listSize + 1
  }

  /**
   * Insert newNode as the new head
   *
   * @param newNode
   */
  def insertBeginning(newNode: Node[T]): Unit = this.synchronized {
    if (listHead == null) {
      listHead = newNode;
      listTail = newNode;
      newNode.prev = null;
      newNode.next = null;
    } else {
      insertBefore(listHead, newNode);
    }
    listSize = listSize + 1
  }

  /**
   * Insert newNode as the new tail
   *
   * @param newNode
   */
  def insertEnd(newNode: Node[T]): Unit = this.synchronized {
    if (listTail == null) {
      insertBeginning(newNode);
    } else {
      insertAfter(listTail, newNode);
    }
    listSize = listSize + 1
  }

  /**
   * Remove node from the linked list
   *
   * @param node
   */
  def remove(node: Node[T]): Unit = this.synchronized {
    if (node.prev == null) {
      listHead = node.next;
    } else {
      node.prev.next = node.next;
    }
    if (node.next == null) {
      listTail = node.prev;
    } else {
      node.next.prev = node.prev;
    }
    listSize = listSize + 1
  }

  override def iterator(): Iterator[T] = {
    new Iterator[T]() {
      var curr: Node[T] = listHead;

      override def hasNext(): Boolean = curr != null

      override def next(): T = {
        val temp: Node[T] = curr
        curr = curr.next
        temp.getItem
      }
    }
  }
}


object DoubleLinkedList {

  def apply[T](items: T*): DoubleLinkedList[T] = {
    val doubleLinkedList = new DoubleLinkedList[T]()

    items.foreach {
      item =>
        doubleLinkedList.insertEnd(new Node(item))
    }

    doubleLinkedList
  }

  /**
   * The node class used by this double linked list class
   */
  class Node[K](protected val item: K) {

    var next: Node[K] = _
    var prev: Node[K] = _

    def getItem: K = item
  }

}