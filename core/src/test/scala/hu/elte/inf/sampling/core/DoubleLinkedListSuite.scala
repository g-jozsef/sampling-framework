package hu.elte.inf.sampling.core

import hu.elte.inf.sampling.core.DoubleLinkedList.Node
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit test for [[DoubleLinkedList]]
 */
class DoubleLinkedListSuite
  extends AnyFunSuite with Matchers {

  test("With a DoubleLinkedList you should be able to add values to the and") {
    val list = DoubleLinkedList(1, 2)
    list.insertEnd(new Node(3))
    val expected = DoubleLinkedList(1, 2, 3)


    list should (contain theSameElementsAs expected)
  }

  test("With a DoubleLinkedList you should be able to add values to the beginning") {
    val list = DoubleLinkedList(2, 3)
    list.insertBeginning(new Node(1))
    val expected = DoubleLinkedList(1, 2, 3)

    list should (contain theSameElementsAs expected)
  }

  test("With a DoubleLinkedList you should be able to remove from the list") {
    val list = new DoubleLinkedList[Int]();
    for (i: Int <- 1 to 3) {
      list.insertEnd(new Node(i));
    }
    val nodeToRemove = new Node(4);
    list.insertEnd(nodeToRemove);

    for (i: Int <- 5 to 8) {
      list.insertEnd(new Node(i));
    }

    list.remove(nodeToRemove);
    val expected = DoubleLinkedList(1, 2, 3, 5, 6, 7, 8);
    list should (contain theSameElementsAs expected)
  }

  test("With a DoubleLinkedList you should be able to use it as an iterator - I") {
    val list = DoubleLinkedList(1, 2, 3, 4, 5, 6, 7);
    var expected = 1;
    list.foreach {
      i =>
        i shouldBe expected
        expected = expected + 1
    }
    //we increment one additional time
    val a = expected
    a should equal(8)
  }

  test("With a DoubleLinkedList you should be able to use it as an iterator - II") {
    val list = DoubleLinkedList(1, 2, 3, 4, 5, 6, 7)
    list.foreach(System.out.println)
    val sum = list.reduce(Integer.sum)

    sum should be(28)
  }

  test("With a DoubleLinkedList you should be able to use it as an iterator - III") {
    val list = DoubleLinkedList(Array(1, 2, 3),
      Array(4, 5),
      Array(6, 7))
    val sum = list.flatMap(x => x.iterator).reduce(Integer.sum)

    sum shouldEqual 28
  }

}
