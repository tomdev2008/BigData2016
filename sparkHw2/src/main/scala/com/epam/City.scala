package com.epam

case class City(
                 id: Int,
                 city: String,
                 stateId: Int,
                 population: Int,
                 area: Float,
                 density: Float,
                 lalitude: Float,
                 longitude: Float) extends Serializable