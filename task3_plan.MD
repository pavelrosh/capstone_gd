== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['campaignId], ['campaignId, 'sum('billingCost) AS revenue#491]
         +- 'Filter (('isConfirmed = TRUE) AND 'purchaseTime LIKE %2019-01%)
            +- 'UnresolvedRelation [projection]

== Analyzed Logical Plan ==
campaignId: string, revenue: double
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#491 DESC NULLS LAST], true
      +- Aggregate [campaignId#214], [campaignId#214, sum(cast(billingCost#54 as double)) AS revenue#491]
         +- Filter ((isConfirmed#55 = TRUE) AND purchaseTime#53 LIKE %2019-01%)
            +- SubqueryAlias projection
               +- Project [purchaseId#52, purchaseTime#53, billingCost#54, isConfirmed#55, campaignId#214, channelId#213, session_id#149]
                  +- Join Inner, (purchaseId#83 = purchaseId#52)
                     :- SubqueryAlias clicks_sessions
                     :  +- Sort [userId#16 ASC NULLS FIRST], true
                     :     +- Project [userId#16, eventId#17, campaignId#214, channelId#213, purchaseId#83, session_id#149]
                     :        +- Join Inner, (((userId#16 = userId#215) AND (start_session#147 < eventTime#18)) AND (end_session#148 > eventTime#18))
                     :           :- SubqueryAlias clicks
                     :           :  +- SubqueryAlias clickstream_purchases
                     :           :     +- Project [userId#16, eventId#17, eventTime#18, eventType#19, campaignId#66, channelId#74, purchaseId#83]
                     :           :        +- Filter (eventType#19 = purchase)
                     :           :           +- SubqueryAlias clickstream
                     :           :              +- Project [userId#16, eventId#17, eventTime#18, eventType#19, campaignId#66, channelId#74, purchaseId#83]
                     :           :                 +- Project [userId#16, eventId#17, eventTime#18, eventType#19, attributes#20, campaignId#66, channelId#74, <lambda>(attributes#20) AS purchaseId#83]
                     :           :                    +- Project [userId#16, eventId#17, eventTime#18, eventType#19, attributes#20, campaignId#66, <lambda>(attributes#20) AS channelId#74]
                     :           :                       +- Project [userId#16, eventId#17, eventTime#18, eventType#19, attributes#20, <lambda>(attributes#20) AS campaignId#66]
                     :           :                          +- Sort [userId#16 ASC NULLS FIRST, eventTime#18 ASC NULLS FIRST], true
                     :           :                             +- Project [userId#16, eventId#17, eventTime#18, eventType#19, attributes#20]
                     :           :                                +- Filter (((eventType#19 = purchase) OR (eventType#19 = app_open)) OR (eventType#19 = app_close))
                     :           :                                   +- SubqueryAlias clickstream
                     :           :                                      +- Relation[userId#16,eventId#17,eventTime#18,eventType#19,attributes#20] csv
                     :           +- SubqueryAlias sessions
                     :              +- Project [userId#215, start_session#147, end_session#148, eventType#218, campaignId#214, channelId#213, session_id#149]
                     :                 +- Filter (eventType#218 = app_open)
                     :                    +- SubqueryAlias __auto_generated_subquery_name
                     :                       +- Sort [userId#215 ASC NULLS FIRST, start_session#147 ASC NULLS FIRST], true
                     :                          +- Project [userId#215, start_session#147, end_session#148, eventType#218, campaignId#214, channelId#213, session_id#149]
                     :                             +- Project [userId#215, start_session#147, eventType#218, campaignId#214, channelId#213, session_id#149, eventTime#217, end_session#148, end_session#148]
                     :                                +- Window [lead(eventTime#217, 1, null) windowspecdefinition(userId#215, eventTime#217 ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS end_session#148], [userId#215], [eventTime#217 ASC NULLS FIRST]
                     :                                   +- Project [userId#215, eventTime#217 AS start_session#147, eventType#218, campaignId#214, channelId#213, eventId#216 AS session_id#149, eventTime#217]
                     :                                      +- SubqueryAlias sessions
                     :                                         +- Project [userId#215, eventId#216, eventType#218, eventTime#217, campaignId#214, channelId#213]
                     :                                            +- Filter ((eventType#218 = app_open) OR (eventType#218 = app_close))
                     :                                               +- SubqueryAlias clickstream
                     :                                                  +- Project [userId#215, eventId#216, eventTime#217, eventType#218, campaignId#214, channelId#213, purchaseId#83]
                     :                                                     +- Project [userId#215, eventId#216, eventTime#217, eventType#218, attributes#219, campaignId#214, channelId#213, <lambda>(attributes#219) AS purchaseId#83]
                     :                                                        +- Project [userId#215, eventId#216, eventTime#217, eventType#218, attributes#219, campaignId#214, <lambda>(attributes#219) AS channelId#213]
                     :                                                           +- Project [userId#215, eventId#216, eventTime#217, eventType#218, attributes#219, <lambda>(attributes#219) AS campaignId#214]
                     :                                                              +- Sort [userId#215 ASC NULLS FIRST, eventTime#217 ASC NULLS FIRST], true
                     :                                                                 +- Project [userId#215, eventId#216, eventTime#217, eventType#218, attributes#219]
                     :                                                                    +- Filter (((eventType#218 = purchase) OR (eventType#218 = app_open)) OR (eventType#218 = app_close))
                     :                                                                       +- SubqueryAlias clickstream
                     :                                                                          +- Relation[userId#215,eventId#216,eventTime#217,eventType#218,attributes#219] csv
                     +- SubqueryAlias purchases
                        +- Relation[purchaseId#52,purchaseTime#53,billingCost#54,isConfirmed#55] csv

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#491 DESC NULLS LAST], true
      +- Aggregate [campaignId#214], [campaignId#214, sum(cast(billingCost#54 as double)) AS revenue#491]
         +- Project [billingCost#54, campaignId#214]
            +- Join Inner, (purchaseId#83 = purchaseId#52)
               :- Project [campaignId#214, purchaseId#83]
               :  +- Join Inner, (((userId#16 = userId#215) AND (start_session#147 < eventTime#18)) AND (end_session#148 > eventTime#18))
               :     :- Project [userId#16, eventTime#18, pythonUDF0#496 AS purchaseId#83]
               :     :  +- BatchEvalPython [<lambda>(attributes#20)], [pythonUDF0#496]
               :     :     +- Project [userId#16, eventTime#18, attributes#20]
               :     :        +- Filter isnotnull(pythonUDF0#495)
               :     :           +- BatchEvalPython [<lambda>(attributes#20)], [pythonUDF0#495]
               :     :              +- Project [userId#16, eventTime#18, eventType#19, attributes#20]
               :     :                 +- Filter (((isnotnull(eventType#19) AND (eventType#19 = purchase)) AND isnotnull(eventTime#18)) AND isnotnull(userId#16))
               :     :                    +- Relation[userId#16,eventId#17,eventTime#18,eventType#19,attributes#20] csv
               :     +- Project [userId#215, start_session#147, end_session#148, campaignId#214]
               :        +- Filter (((isnotnull(eventType#218) AND (eventType#218 = app_open)) AND isnotnull(start_session#147)) AND isnotnull(end_session#148))
               :           +- Window [lead(eventTime#217, 1, null) windowspecdefinition(userId#215, eventTime#217 ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS end_session#148], [userId#215], [eventTime#217 ASC NULLS FIRST]
               :              +- Project [userId#215, eventTime#217 AS start_session#147, eventType#218, pythonUDF0#497 AS campaignId#214, eventTime#217]
               :                 +- BatchEvalPython [<lambda>(attributes#219)], [pythonUDF0#497]
               :                    +- Sort [userId#215 ASC NULLS FIRST, eventTime#217 ASC NULLS FIRST], true
               :                       +- Project [userId#215, eventTime#217, eventType#218, attributes#219]
               :                          +- Filter (((eventType#218 = app_open) OR (eventType#218 = app_close)) AND isnotnull(userId#215))
               :                             +- Relation[userId#215,eventId#216,eventTime#217,eventType#218,attributes#219] csv
               +- Project [purchaseId#52, billingCost#54]
                  +- Filter ((((isnotnull(isConfirmed#55) AND isnotnull(purchaseTime#53)) AND (isConfirmed#55 = TRUE)) AND Contains(purchaseTime#53, 2019-01)) AND isnotnull(purchaseId#52))
                     +- Relation[purchaseId#52,purchaseTime#53,billingCost#54,isConfirmed#55] csv

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#491 DESC NULLS LAST], output=[campaignId#214,revenue#491])
+- *(10) HashAggregate(keys=[campaignId#214], functions=[sum(cast(billingCost#54 as double))], output=[campaignId#214, revenue#491])
   +- Exchange hashpartitioning(campaignId#214, 200), true, [id=#1583]
      +- *(9) HashAggregate(keys=[campaignId#214], functions=[partial_sum(cast(billingCost#54 as double))], output=[campaignId#214, sum#499])
         +- *(9) Project [billingCost#54, campaignId#214]
            +- *(9) BroadcastHashJoin [purchaseId#83], [purchaseId#52], Inner, BuildRight
               :- *(9) Project [campaignId#214, purchaseId#83]
               :  +- *(9) BroadcastHashJoin [userId#16], [userId#215], Inner, BuildLeft, ((start_session#147 < eventTime#18) AND (end_session#148 > eventTime#18))
               :     :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true])), [id=#1549]
               :     :  +- *(3) Project [userId#16, eventTime#18, pythonUDF0#496 AS purchaseId#83]
               :     :     +- BatchEvalPython [<lambda>(attributes#20)], [pythonUDF0#496]
               :     :        +- *(2) Project [userId#16, eventTime#18, attributes#20]
               :     :           +- *(2) Filter isnotnull(pythonUDF0#495)
               :     :              +- BatchEvalPython [<lambda>(attributes#20)], [pythonUDF0#495]
               :     :                 +- *(1) Project [userId#16, eventTime#18, eventType#19, attributes#20]
               :     :                    +- *(1) Filter (((isnotnull(eventType#19) AND (eventType#19 = purchase)) AND isnotnull(eventTime#18)) AND isnotnull(userId#16))
               :     :                       +- FileScan csv [userId#16,eventTime#18,eventType#19,attributes#20] Batched: false, DataFilters: [isnotnull(eventType#19), (eventType#19 = purchase), isnotnull(eventTime#18), isnotnull(userId#16)], Format: CSV, Location: InMemoryFileIndex[file:/Users/proshchyna/PycharmProjects/capstone/input_csv_datasets/mobile-app-c..., PartitionFilters: [], PushedFilters: [IsNotNull(eventType), EqualTo(eventType,purchase), IsNotNull(eventTime), IsNotNull(userId)], ReadSchema: struct<userId:string,eventTime:string,eventType:string,attributes:string>
               :     +- *(9) Project [userId#215, start_session#147, end_session#148, campaignId#214]
               :        +- *(9) Filter (((isnotnull(eventType#218) AND (eventType#218 = app_open)) AND isnotnull(start_session#147)) AND isnotnull(end_session#148))
               :           +- Window [lead(eventTime#217, 1, null) windowspecdefinition(userId#215, eventTime#217 ASC NULLS FIRST, specifiedwindowframe(RowFrame, 1, 1)) AS end_session#148], [userId#215], [eventTime#217 ASC NULLS FIRST]
               :              +- *(7) Sort [userId#215 ASC NULLS FIRST, eventTime#217 ASC NULLS FIRST], false, 0
               :                 +- Exchange hashpartitioning(userId#215, 200), true, [id=#1563]
               :                    +- *(6) Project [userId#215, eventTime#217 AS start_session#147, eventType#218, pythonUDF0#497 AS campaignId#214, eventTime#217]
               :                       +- BatchEvalPython [<lambda>(attributes#219)], [pythonUDF0#497]
               :                          +- *(5) Sort [userId#215 ASC NULLS FIRST, eventTime#217 ASC NULLS FIRST], true, 0
               :                             +- Exchange rangepartitioning(userId#215 ASC NULLS FIRST, eventTime#217 ASC NULLS FIRST, 200), true, [id=#1555]
               :                                +- *(4) Project [userId#215, eventTime#217, eventType#218, attributes#219]
               :                                   +- *(4) Filter (((eventType#218 = app_open) OR (eventType#218 = app_close)) AND isnotnull(userId#215))
               :                                      +- FileScan csv [userId#215,eventTime#217,eventType#218,attributes#219] Batched: false, DataFilters: [((eventType#218 = app_open) OR (eventType#218 = app_close)), isnotnull(userId#215)], Format: CSV, Location: InMemoryFileIndex[file:/Users/proshchyna/PycharmProjects/capstone/input_csv_datasets/mobile-app-c..., PartitionFilters: [], PushedFilters: [Or(EqualTo(eventType,app_open),EqualTo(eventType,app_close)), IsNotNull(userId)], ReadSchema: struct<userId:string,eventTime:string,eventType:string,attributes:string>
               +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true])), [id=#1577]
                  +- *(8) Project [purchaseId#52, billingCost#54]
                     +- *(8) Filter ((((isnotnull(isConfirmed#55) AND isnotnull(purchaseTime#53)) AND (isConfirmed#55 = TRUE)) AND Contains(purchaseTime#53, 2019-01)) AND isnotnull(purchaseId#52))
                        +- FileScan csv [purchaseId#52,purchaseTime#53,billingCost#54,isConfirmed#55] Batched: false, DataFilters: [isnotnull(isConfirmed#55), isnotnull(purchaseTime#53), (isConfirmed#55 = TRUE), Contains(purchas..., Format: CSV, Location: InMemoryFileIndex[file:/Users/proshchyna/PycharmProjects/capstone/input_csv_datasets/purchases_sa..., PartitionFilters: [], PushedFilters: [IsNotNull(isConfirmed), IsNotNull(purchaseTime), EqualTo(isConfirmed,TRUE), StringContains(purch..., ReadSchema: struct<purchaseId:string,purchaseTime:string,billingCost:string,isConfirmed:string>
