# Shipment KPIs and Business Insights

## Gold KPI tables

| Table | Business question |
| --- | --- |
| `meridian_kpi_monthly_delivery` | What share of delivered shipments arrived on time, and how long did transit take? |
| `meridian_kpi_carrier_scorecard` | Which carriers met their on-time SLA target? |
| `meridian_kpi_lane_performance` | How do origin-to-destination routes compare on volume, revenue per kilogram, and delivery performance? |
| `meridian_kpi_delay_reasons` | Which reasons account for late deliveries by month and carrier? |

The notebook publishes each KPI in Delta and in an Iceberg counterpart with the `_iceberg` suffix. [Notebook](../Data%20Ingestion/Batch/01_Shipments_Batch.scala) · [Parity evidence](../Testing/evidence/databricks-kpi-comparison.csv)

Use the [business SQL queries](meridian-business-queries.sql) for shipment status counts, monthly freight revenue by customer segment, seasonal delay mix, and rejection review. The [business insight summary](project-insights.md) pairs the three mini-pack findings with proposed actions.

These Gold KPI tables are batch outputs from the historical shipment snapshot. Continuous tracking events are validated separately in the streaming Delta audit and five-minute Iceberg windows; they are not automatically merged into the historical KPI tables.
