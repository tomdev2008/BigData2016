package com.epam

import java.util.Date

case class StreamRecord (
                          bidId: String,
                          timestamp: Date,
                          iPinyouId: String,
                          userAgent: String,
                          ip: String,
                          region: String,
                          city: Int,
                          adExchange: String,
                          domain: String,
                          url: String,
                          anonymous: String,
                          adSlotId: String,
                          adSlotWidth: String,
                          adSlotHeight: String,
                          adSlotVisibility: String,
                          adSlotFormat: String,
                          adSlotFloorPrice: String,
                          creativeId: String,
                          biddingPrice: String,
                          advertiser: String,
                          userTags: String,
                          streamId: Int) extends Serializable
