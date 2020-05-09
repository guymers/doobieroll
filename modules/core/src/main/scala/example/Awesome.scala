package example

import example.Better.{mkEmptyIdMap, AnyKeyMultiDict, Atom, EE, LookupByCatKey}
import shapeless.{::, HList}
import cats.implicits._

import scala.collection.{mutable, MapView}

object Awesome {

  // A is for type inference
  trait IdAtom[A, Id, Adb] {
    def getId(adb: Adb): Id
  }

  trait Vis[A, Dbs <: HList] {
    def recordAsChild(parentId: Any, d: Dbs): Unit

    def assemble(): collection.MapView[Any, Vector[Either[EE, A]]]
  }

  trait ParVis[A, Dbs <: HList] extends Vis[A, Dbs] {
    def recordTopLevel(dbs: Dbs): Unit
    def assembleTopLevel(): Vector[Either[EE, A]]
  }

  trait MkVis[A, Dbs <: HList] {
    def mkVis(accum: Aqum, catKey: String): Vis[A, Dbs]
  }

  trait MkParVis[A, Dbs <: HList] extends MkVis[A, Dbs] {
    override def mkVis(accum: Aqum, catKey: String): ParVis[A, Dbs]
  }

  def mkVisAtom[A, Dbs <: HList](
    dbDesc: Atom[A, Dbs]
  ): MkVis[A, Dbs] = new MkVis[A, Dbs] {

    override def mkVis(accum: Aqum, catKey: String): Vis[A, Dbs] = new Vis[A, Dbs] {
      override def recordAsChild(parentId: Any, d: Dbs): Unit =
        accum.addRaw(catKey, parentId, d)

      override def assemble(): collection.MapView[Any, Vector[Either[EE, A]]] =
        accum
          .getRawLookup(catKey)
          .sets
          .view
          .mapValues(valueSet => valueSet.toVector.map(v => dbDesc.construct(v.asInstanceOf[Dbs])))
    }

  }

  def mkVisParent[A, Id, ADb, C, CDb <: HList](
    idAtom: IdAtom[A, Id, ADb],
    mkVisChild: MkVis[C, CDb],
    constructWithChild: (ADb, Vector[C]) => Either[EE, A]
  ): MkParVis[A, ADb :: CDb] = new MkParVis[A, ADb :: CDb] {

    override def mkVis(accum: Aqum, catKey: String): ParVis[A, ADb :: CDb] =
      new ParVis[A, ADb :: CDb] {

        val childCatKey = s"$catKey.0"
        val visChild = mkVisChild.mkVis(accum, childCatKey)

        override def recordAsChild(parentId: Any, d: ADb :: CDb): Unit = {
          val adb :: cdb = d
          accum.addRaw(catKey, parentId, adb)
          val id = idAtom.getId(adb)
          visChild.recordAsChild(parentId = id, cdb)
        }

        override def recordTopLevel(dbs: ADb :: CDb): Unit = {
          val adb :: cdb = dbs
          val thisId = idAtom.getId(adb)
          accum.addToTopLevel(thisId, adb)
          visChild.recordAsChild(parentId = thisId, cdb)
        }

        override def assemble(): collection.MapView[Any, Vector[Either[EE, A]]] = {
          accum.getRawLookup(catKey).sets.view.mapValues { valueSet =>
            val childValues = visChild.assemble()
            valueSet.toVector.map { v =>
              val rawAdb = v.asInstanceOf[ADb]
              val thisId = idAtom.getId(rawAdb)
              for {
                thisChildren <- childValues.getOrElse(thisId, Vector.empty).sequence
                a <- constructWithChild(rawAdb, thisChildren)
              } yield a
            }
          }
        }

        override def assembleTopLevel(): Vector[Either[EE, A]] = {
          accum.getTopLevel[ADb].map { adb =>
            val childValues = visChild.assemble()
            val thisId = idAtom.getId(adb)
            for {
              thisChildren <- childValues.getOrElse(thisId, Vector.empty).sequence
              a <- constructWithChild(adb, thisChildren)
            } yield a
          }
        }.toVector
      }

  }

  def optVis[A, ADb, CDb <: HList](
    previs: MkVis[A, ADb :: CDb]
  ): MkVis[A, Option[ADb] :: CDb] = new MkVis[A, Option[ADb] :: CDb] {
    override def mkVis(accum: Aqum, catKey: String): Vis[A, Option[ADb] :: CDb] = {

      val underlying = previs.mkVis(accum, catKey)

      new Vis[A, Option[ADb] :: CDb] {
        override def recordAsChild(
          parentId: Any,
          d: Option[ADb] :: CDb
        ): Unit =
          d.head.foreach { adb =>
            underlying.recordAsChild(parentId, adb :: d.tail)
          }

        override def assemble(): MapView[Any, Vector[Either[EE, A]]] =
          underlying.assemble()
      }
    }

  }

  def assembleUnordered[A, Dbs <: HList](
    rows: Vector[Dbs],
    mkParVis: MkParVis[A, Dbs]
  ): Vector[Either[EE, A]] = {
    if (rows.isEmpty) return Vector.empty
    val accum = Aqum.mkEmpty()
    val catKey = "t"

    val parVis = mkParVis.mkVis(accum, catKey)

    rows.foreach { dbs =>
      parVis.recordTopLevel(dbs)
    }

    parVis.assembleTopLevel()
  }

  // FIXME: need optParVis

  class Aqum private (
    topLevelDbItem: mutable.Map[Any, Any],
    // For storing raw parent DB values because all child isn't availble yet
    rawLookup: LookupByCatKey[Any],
  ) {
    def addToTopLevel(k: Any, v: Any): Unit =
      topLevelDbItem.update(k, v)

    def getTopLevel[A]: Iterator[A] =
      topLevelDbItem.iterator.map(_._2).asInstanceOf[Iterator[A]]

    def addRaw(
      catKey: String,
      id: Any,
      value: Any
    ): Unit = {
      val idMap = rawLookup.getOrElseUpdate(
        catKey,
        mkEmptyIdMap()
      )
      idMap.addOne(id -> value)
    }

    def getRawLookup(
      catKey: String
    ): mutable.MultiDict[Any, Any] =
      rawLookup.getOrElse(catKey, sys.error(s"getRaw for $catKey not found"))

  }

  object Aqum {
    def mkEmpty(): Aqum = new Aqum(
      topLevelDbItem = mutable.Map.empty[Any, Any],
      rawLookup = mutable.Map.empty[String, AnyKeyMultiDict[Any]]
    )
  }

}