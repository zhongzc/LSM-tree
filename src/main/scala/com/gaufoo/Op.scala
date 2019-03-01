package com.gaufoo

abstract class Op(val op: String, val params: String*)
final case class SetOp(key: String, value: String) extends Op("set", key, value)
final case class GetOp(key: String) extends Op("get", key)
final case class DelOp(key: String) extends Op("del", key)
