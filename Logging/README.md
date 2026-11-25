# Execution Logs and Recovery Evidence

| Evidence | What to look for |
| --- | --- |
| [Kubernetes batch log](meridian-batch-kubernetes.log) | `Batch verified` and the shipment reconciliation |
| [Local replay](meridian-stream-before-restart.log) and [restart](meridian-stream-after-restart.log) | 43 saved events and the same 12 windows after restart |
| [ADLS recovery log](meridian-adls-recovery-verification.log) | 44 restored records, one new event, and the final 45-record audit |
| [Kafka lag measurement](../Testing/evidence/meridian-kafka-lag.csv) | Committed Spark and Kafka end offsets both at 45 |
| [Producer rollout](../Testing/evidence/kubernetes-producer-rollout.log) | Deployment update, rollback, and readiness |
| [Fresh rerun attempt](../Testing/evidence/meridian-final-demo-attempt.log) | Local Docker Desktop timeout before the ADLS stage |

The files above are archived evidence from earlier executions. The local continuous Compose demo writes current consumer output to `docker compose -f Development/Docker/compose.realtime.yaml logs -f stream`; it does not create or overwrite these archived logs.

The [lag utility](../Development/Scripts/measure-meridian-kafka-lag.ps1) compares committed Spark offsets with broker end offsets. See the [demo guide](../docs/demo-guide.md#inspect-a-failed-deployment) for inspecting Kubernetes logs and events.

For evaluation, start with shipment reconciliation, then show the replacement pod's startup state and the final Kafka lag. The 43-event PVC replay and the 45-event ADLS audit come from separate runs.
