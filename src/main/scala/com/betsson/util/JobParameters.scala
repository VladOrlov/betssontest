package com.betsson.util

case class JobParameters(
                          baseInputPath: String,
                          executionPartitions: Int,
                          writePartitions: Int,
                          outputPath: String
                        )