package spark.jobs

object Test extends App {
  class Inta(asd: Int) {
    def withInt(f: Int => String): String = f(asd)
  }

  val a = new Inta(1)
  val r = a.withInt(i => s"asd $i")
  println(r)
}
