class Node[T](var data: T, var next: Option[Node[T]] = None)

class LinkedList[T] {
  var head: Option[Node[T]] = None

  def append(data: T): Unit = {
    val newNode = new Node(data)
    head match {
      case None =>
        head = Some(newNode)
      case Some(_) =>
        var current = head
        while (current.get.next.isDefined) {
          current = current.get.next
        }
        current.get.next = Some(newNode)
    }
  }

  def display(): Unit = {
    var current = head
    while (current.isDefined) {
      print(current.get.data + " -> ")
      current = current.get.next
    }
    println("null")
  }

  def delete(data: T): Unit = {
    head = deleteNode(head, data)
  }

  private def deleteNode(node: Option[Node[T]], data: T): Option[Node[T]] = {
    node match {
      case None => None
      case Some(current) =>
        if (current.data == data) {
          current.next
        } else {
          current.next = deleteNode(current.next, data)
          Some(current)
        }
    }
  }
}

object LinkedListApp {
  def main(args: Array[String]): Unit = {
    val linkedList = new LinkedList[Int]

    linkedList.append(10)
    linkedList.append(20)
    linkedList.append(30)

    println("Linked List:")
    linkedList.display()

    linkedList.delete(10)
    println("Linked List after deleting 10:")
    linkedList.display()
  }
}
