== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'sum('billingCost) AS revenue#423]
         +- 'Filter (('isConfirmed = TRUE) AND 'purchaseTime LIKE %2019-01%)
            +- 'UnresolvedRelation [projection]

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#423 DESC NULLS LAST], true
      +- Aggregate [campaignId#223], [campaignId#223, sum(cast(billingCost#49 as double)) AS revenue#423]
         +- Filter ((isConfirmed#50 = TRUE) AND purchaseTime#48 LIKE %2019-01%)
            +- SubqueryAlias projection
               +- Project [purchaseId#47, purchaseTime#48, billingCost#49, isConfirmed#50, campaignId#223, channelId#222, session_id#145]
                  +- Join Inner, (purchaseId#116 = purchaseId#47)
                     :- SubqueryAlias clicks_sessions
                     :  +- Sort [userId#80 ASC NULLS FIRST], true
                     :     +- Project [userId#80, eventId#81, campaignId#223, channelId#222, purchaseId#116, session_id#145]
                     :        +- Join Inner, (((userId#80 = userId#224) AND (start_session#143 < eventTime#84)) AND (end_session#144 > eventTime#84))
                     :           :- SubqueryAlias clicks
                     :           :  +- SubqueryAlias clickstream_purchases
                     :           :     +- Filter (eventType#82 = purchase)
                     :           :        +- Project [userId#80, eventId#81, eventType#82, eventTime#84, campaignId#99, channelId#107, purchaseId#116]
                     :           :           +- Project [userId#80, eventId#81, eventType#82, attributes#83, eventTime#84, campaignId#99, channelId#107, <lambda>(attributes#83) AS purchaseId#116]
                     :           :              +- Project [userId#80, eventId#81, eventType#82, attributes#83, eventTime#84, campaignId#99, <lambda>(attributes#83) AS channelId#107]
                     :           :                 +- Project [userId#80, eventId#81, eventType#82, attributes#83, eventTime#84, <lambda>(attributes#83) AS campaignId#99]
                     :           :                    +- Relation[userId#80,eventId#81,eventType#82,attributes#83,eventTime#84] parquet
                     :           +- SubqueryAlias sessions
                     :              +- Project [userId#224, start_session#143, end_session#144, eventType#226, campaignId#223, channelId#222, session_id#145]
                     :                 +- Filter (eventType#226 = app_open)
                     :                    +- SubqueryAlias __auto_generated_subquery_name
                     :                       +- Sort [userId#224 ASC NULLS FIRST, start_session#143 ASC NULLS FIRST], true
                     :                          +- Project [userId#224, start_session#143, end_session#144, eventType#226, campaignId#223, channelId#222, session_id#145]
                     :                             +- Project [userId#224, start_session#143, eventType#226, campaignId#223, channelId#222, session_id#145, eventTime#228, end_session#144, end_session#144]
                     :                                +- Window [lead(eventTime#228, 1, null) windowspecdefinition(userId#224, eventTime#228 ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS end_session#144], [userId#224], [eventTime#228 ASC NULLS FIRST]
                     :                                   +- Project [userId#224, eventTime#228 AS start_session#143, eventType#226, campaignId#223, channelId#222, eventId#225 AS session_id#145, eventTime#228]
                     :                                      +- SubqueryAlias sessions
                     :                                         +- Project [userId#224, eventId#225, eventType#226, eventTime#228, campaignId#223, channelId#222]
                     :                                            +- Filter ((eventType#226 = app_open) OR (eventType#226 = app_close))
                     :                                               +- SubqueryAlias clickstream
                     :                                                  +- Project [userId#224, eventId#225, eventType#226, eventTime#228, campaignId#223, channelId#222, purchaseId#116]
                     :                                                     +- Project [userId#224, eventId#225, eventType#226, attributes#227, eventTime#228, campaignId#223, channelId#222, <lambda>(attributes#227) AS purchaseId#116]
                     :                                                        +- Project [userId#224, eventId#225, eventType#226, attributes#227, eventTime#228, campaignId#223, <lambda>(attributes#227) AS channelId#222]
                     :                                                           +- Project [userId#224, eventId#225, eventType#226, attributes#227, eventTime#228, <lambda>(attributes#227) AS campaignId#223]
                     :                                                              +- Relation[userId#224,eventId#225,eventType#226,attributes#227,eventTime#228] parquet
                     +- SubqueryAlias purchases
                        +- Relation[purchaseId#47,purchaseTime#48,billingCost#49,isConfirmed#50] csv

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#423 DESC NULLS LAST], true
      +- Aggregate [campaignId#223], [campaignId#223, sum(cast(billingCost#49 as double)) AS revenue#423]
         +- Project [billingCost#49, campaignId#223]
            +- Join Inner, (purchaseId#116 = purchaseId#47)
               :- Project [campaignId#223, purchaseId#116]
               :  +- Join Inner, (((userId#80 = userId#224) AND (start_session#143 < eventTime#84)) AND (end_session#144 > eventTime#84))
               :     :- Project [userId#80, eventTime#84, pythonUDF0#428 AS purchaseId#116]
               :     :  +- BatchEvalPython [<lambda>(attributes#83)], [pythonUDF0#428]
               :     :     +- Project [userId#80, attributes#83, eventTime#84]
               :     :        +- Filter isnotnull(pythonUDF0#427)
               :     :           +- BatchEvalPython [<lambda>(attributes#83)], [pythonUDF0#427]
               :     :              +- Project [userId#80, eventType#82, attributes#83, eventTime#84]
               :     :                 +- Filter (((isnotnull(eventType#82) AND (eventType#82 = purchase)) AND isnotnull(eventTime#84)) AND isnotnull(userId#80))
               :     :                    +- Relation[userId#80,eventId#81,eventType#82,attributes#83,eventTime#84] parquet
               :     +- Project [userId#224, start_session#143, end_session#144, campaignId#223]
               :        +- Filter (((isnotnull(eventType#226) AND (eventType#226 = app_open)) AND isnotnull(start_session#143)) AND isnotnull(end_session#144))
               :           +- Window [lead(eventTime#228, 1, null) windowspecdefinition(userId#224, eventTime#228 ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS end_session#144], [userId#224], [eventTime#228 ASC NULLS FIRST]
               :              +- Project [userId#224, eventTime#228 AS start_session#143, eventType#226, pythonUDF0#429 AS campaignId#223, eventTime#228]
               :                 +- BatchEvalPython [<lambda>(attributes#227)], [pythonUDF0#429]
               :                    +- Project [userId#224, eventType#226, attributes#227, eventTime#228]
               :                       +- Filter (((eventType#226 = app_open) OR (eventType#226 = app_close)) AND isnotnull(userId#224))
               :                          +- Relation[userId#224,eventId#225,eventType#226,attributes#227,eventTime#228] parquet
               +- Project [purchaseId#47, billingCost#49]
                  +- Filter ((((isnotnull(isConfirmed#50) AND isnotnull(purchaseTime#48)) AND (isConfirmed#50 = TRUE)) AND Contains(purchaseTime#48, 2019-01)) AND isnotnull(purchaseId#47))
                     +- Relation[purchaseId#47,purchaseTime#48,billingCost#49,isConfirmed#50] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#423 DESC NULLS LAST], output=[campaignId#223,revenue#423])
+- *(9) HashAggregate(keys=[campaignId#223], functions=[sum(cast(billingCost#49 as double))], output=[campaignId#223, revenue#423])
   +- Exchange hashpartitioning(campaignId#223, 200), true, [id=#1739]
      +- *(8) HashAggregate(keys=[campaignId#223], functions=[partial_sum(cast(billingCost#49 as double))], output=[campaignId#223, sum#431])
         +- *(8) Project [billingCost#49, campaignId#223]
            +- *(8) BroadcastHashJoin [purchaseId#116], [purchaseId#47], Inner, BuildRight
               :- *(8) Project [campaignId#223, purchaseId#116]
               :  +- *(8) BroadcastHashJoin [userId#80], [userId#224], Inner, BuildLeft, ((start_session#143 < eventTime#84) AND (end_session#144 > eventTime#84))
               :     :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true])), [id=#1708]
               :     :  +- *(3) Project [userId#80, eventTime#84, pythonUDF0#428 AS purchaseId#116]
               :     :     +- BatchEvalPython [<lambda>(attributes#83)], [pythonUDF0#428]
               :     :        +- *(2) Project [userId#80, attributes#83, eventTime#84]
               :     :           +- *(2) Filter isnotnull(pythonUDF0#427)
               :     :              +- BatchEvalPython [<lambda>(attributes#83)], [pythonUDF0#427]
               :     :                 +- *(1) Project [userId#80, eventType#82, attributes#83, eventTime#84]
               :     :                    +- *(1) Filter ((isnotnull(eventType#82) AND (eventType#82 = purchase)) AND isnotnull(userId#80))
               :     :                       +- *(1) ColumnarToRow
               :     :                          +- FileScan parquet [userId#80,eventType#82,attributes#83,eventTime#84] Batched: true, DataFilters: [isnotnull(eventType#82), (eventType#82 = purchase), isnotnull(userId#80)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/proshchyna/PycharmProjects/capstone/input_dataset/clickstream.parquet], PartitionFilters: [isnotnull(eventTime#84)], PushedFilters: [IsNotNull(eventType), EqualTo(eventType,purchase), IsNotNull(userId)], ReadSchema: struct<userId:string,eventType:string,attributes:string>
               :     +- *(8) Project [userId#224, start_session#143, end_session#144, campaignId#223]
               :        +- *(8) Filter (((isnotnull(eventType#226) AND (eventType#226 = app_open)) AND isnotnull(start_session#143)) AND isnotnull(end_session#144))
               :           +- Window [lead(eventTime#228, 1, null) windowspecdefinition(userId#224, eventTime#228 ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS end_session#144], [userId#224], [eventTime#228 ASC NULLS FIRST]
               :              +- *(6) Sort [userId#224 ASC NULLS FIRST, eventTime#228 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#224, 200), true, [id=#1719]
               :                    +- *(5) Project [userId#224, eventTime#228 AS start_session#143, eventType#226, pythonUDF0#429 AS campaignId#223, eventTime#228]
               :                       +- BatchEvalPython [<lambda>(attributes#227)], [pythonUDF0#429]
               :                          +- *(4) Project [userId#224, eventType#226, attributes#227, eventTime#228]
               :                             +- *(4) Filter (((eventType#226 = app_open) OR (eventType#226 = app_close)) AND isnotnull(userId#224))
               :                                +- *(4) ColumnarToRow
               :                                   +- FileScan parquet [userId#224,eventType#226,attributes#227,eventTime#228] Batched: true, DataFilters: [((eventType#226 = app_open) OR (eventType#226 = app_close)), isnotnull(userId#224)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/proshchyna/PycharmProjects/capstone/input_dataset/clickstream.parquet], PartitionFilters: [], PushedFilters: [Or(EqualTo(eventType,app_open),EqualTo(eventType,app_close)), IsNotNull(userId)], ReadSchema: struct<userId:string,eventType:string,attributes:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true])), [id=#1733]
                  +- *(7) Project [purchaseId#47, billingCost#49]
                     +- *(7) Filter ((((isnotnull(isConfirmed#50) AND isnotnull(purchaseTime#48)) AND (isConfirmed#50 = TRUE)) AND Contains(purchaseTime#48, 2019-01)) AND isnotnull(purchaseId#47))
                        +- FileScan csv [purchaseId#47,purchaseTime#48,billingCost#49,isConfirmed#50] Batched: false, DataFilters: [isnotnull(isConfirmed#50), isnotnull(purchaseTime#48), (isConfirmed#50 = TRUE), Contains(purchas..., Format: CSV, Location: InMemoryFileIndex[file:/Users/proshchyna/PycharmProjects/capstone/purchases_sample.csv], PartitionFilters: [], PushedFilters: [IsNotNull(isConfirmed), IsNotNull(purchaseTime), EqualTo(isConfirmed,TRUE), StringContains(purch..., ReadSchema: struct<purchaseId:string,purchaseTime:string,billingCost:string,isConfirmed:string>
