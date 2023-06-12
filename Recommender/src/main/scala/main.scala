import scala.collection.mutable.ListBuffer

class Node(var value: Int, var retain: Boolean) {
    var nexts = ListBuffer[Node]()

    def this(v: Int, r: Boolean, nexts: ListBuffer[Node]) {
        this(v, r)
        this.nexts = nexts
    }
}

object RetainTree {
    def retain(x: Node): Option[Node] = {
        if (x.nexts.isEmpty) {
            if (x.retain) Some(x) else None
        } else {
            val newNexts = x.nexts.flatMap(retain)
            if (newNexts.nonEmpty || x.retain) {
                Some(new Node(x.value, x.retain, newNexts.to(ListBuffer)))
            } else {
                None
            }
        }
    }

    def preOrderPrint(head: Node): Unit = {
        println(head.value)
        head.nexts.foreach(preOrderPrint)
    }

    def main(args: Array[String]): Unit = {
        val n1 = new Node(1, false)
        val n2 = new Node(2, true)
        val n3 = new Node(3, false)
        val n4 = new Node(4, false)
        val n5 = new Node(5, false)
        val n6 = new Node(6, true)
        val n7 = new Node(7, true)
        val n8 = new Node(8, false)
        val n9 = new Node(9, false)
        val n10 = new Node(10, false)
        val n11 = new Node(11, false)
        val n12 = new Node(12, false)
        val n13 = new Node(13, true)

        n1.nexts = ListBuffer(n2, n3)
        n2.nexts = ListBuffer(n4, n5)
        n3.nexts = ListBuffer(n6, n7)
        n6.nexts = ListBuffer(n8, n9, n10)
        n7.nexts = ListBuffer(n11, n12)
        n9.nexts = ListBuffer(n13)

        val head = retain(n1)
        preOrderPrint(head.get)
    }
}
