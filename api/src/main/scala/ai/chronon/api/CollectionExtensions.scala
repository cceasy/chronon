package ai.chronon.api

object CollectionExtensions {

  implicit class JListExtension[T](list: java.util.List[T]) {

    // we used to have to write a lot of boilerplate like below
    // `Option(jList).foreach(_.iterator().toScala.foreach(f))`
    // This is because `jList` can be null, and we don't want to create intermediate collections
    // The method below will handle all of that and allow us to write `jList.foreach(f)`
    def foreach(f: T => Unit): Unit = {
      if (list == null) return

      val iter = list.iterator()
      while (iter.hasNext) {
        f(iter.next())
      }

    }

    // we used to have to write a lot of boilerplate like below
    // `Option(jList).map(_.iterator().toScala.map(f).toSeq).getOrElse(Seq.empty)`
    // This is because `jList` can be null, and we don't want to create intermediate collections
    // The method below will handle all of that and allow us to write `jList.map(f)`
    def map[U](f: T => U): Iterator[U] = {

      if (list == null) return Iterator.empty

      val iter = list.iterator()
      new Iterator[U] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): U = f(iter.next())
      }

    }

    def flatMap[U](f: T => Iterator[U]): Iterator[U] = {

      if (list == null) return Iterator.empty

      val iter = list.iterator()
      new Iterator[U] {
        private var current: Iterator[U] = Iterator.empty

        override def hasNext: Boolean = {
          while (!current.hasNext && iter.hasNext) {
            current = f(iter.next())
          }
          current.hasNext
        }

        override def next(): U = {
          while (!current.hasNext && iter.hasNext) {
            current = f(iter.next())
          }
          current.next()
        }

      }
    }
  }

  implicit class IteratorExtensions[T](it: Iterator[T]) {

    def distinct: Seq[T] = {

      if (it == null) return Seq.empty

      val set = scala.collection.mutable.HashSet.empty[T]
      while (it.hasNext) {
        set.add(it.next())
      }

      set.toSeq
    }
  }

  implicit class JMapExtension[K, V >: Null](map: java.util.Map[K, V]) {

    def safeGet(key: K, default: V = null): Option[V] = {
      if (map == null) return Option(default)

      val value = map.get(key)
      if (value == null) Option(default) else Some(value)
    }

  }
}
