package com.betsson.model

import java.sql.Timestamp

case class CustomerBalance(customerId: String,
                            balance: Double,
                            calculationDate: Timestamp)
