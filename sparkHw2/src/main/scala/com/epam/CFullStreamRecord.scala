package com.epam

import java.util.Date

case class CFullStreamRecord (
                               bid_id: String,
                               timestamp: Date,
                               i_pinyou_id: String,
                               user_agent: String,
                               ip: String,
                               region: String,
                               ad_exchange: String,
                               domain: String,
                               url: String,
                               anonymous: String,
                               ad_slot_id: String,
                               ad_slot_width: String,
                               ad_slot_height: String,
                               ad_slot_visibility: String,
                               ad_slot_format: String,
                               ad_slot_floor_price: String,
                               creative_id: String,
                               bidding_price: String,
                               advertiser: String,
                               user_tags: String,
                               stream_id: Int,
                               stream_type: String,
                               tags: String,
                               city: String,
                               state_id: Int,
                               population: Int,
                               area: Float,
                               density: Float,
                               latitude: Float,
                               longitude: Float
) extends Serializable
