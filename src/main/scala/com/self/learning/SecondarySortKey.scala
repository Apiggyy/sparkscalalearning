package com.self.learning

class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0) return this.first - that.first;
    else this.second - that.second;
  }
}
