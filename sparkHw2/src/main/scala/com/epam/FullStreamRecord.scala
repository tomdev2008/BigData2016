package com.epam

import java.util.Date

case class FullStreamRecord(
                             record: StreamRecord,
                             city: City,
                             logType: LogType,
                             tags: Tags
                           ) extends Serializable
