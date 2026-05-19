# Fault Tolerance Test

## Test Procedure

### 1. Initial State — Cluster Healthy

```bash
# All three brokers running
docker compose up -d
docker ps --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}"
```

Output (all three brokers UP):
```
kafka1   Up 2 minutes
kafka2   Up 2 minutes
kafka3   Up 2 minutes
```

### 2. Topic Description — Before Failure

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Expected output:
```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

### 3. Stop One Broker

```bash
docker stop kafka2
```

Verify kafka2 is down:
```bash
docker ps --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}"
```

Output:
```
kafka1   Up 5 minutes
kafka3   Up 5 minutes
```

### 4. Topic Description — After Failure

After waiting ~10 seconds for leader re-election:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

Expected output (leader re-elected for partitions previously on kafka2):
```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3
  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1
```

Key changes observed:
- Partition 1 leader changed from broker 2 → broker 3
- ISR shrinks: kafka2 removed from in-sync replicas
- The cluster remains operational with 2 out of 3 brokers

### 5. Producer Test During Degradation

```bash
python src/producer.py --count 50 --rate 10
```

The producer continues to work successfully with `acks=all` because:
- `min.insync.replicas=2` is still satisfied by the 2 remaining brokers
- 3 active partitions remain available

### 6. Restore the Broker

```bash
docker start kafka2
```

After ~15 seconds, kafka2 rejoins the ISR:
```
Topic: sensor-events  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

## Conclusion

The 3-broker Kraft cluster with `replication-factor=3` and `min.insync.replicas=2` tolerates a single broker failure:
- Leader re-election completes automatically within seconds
- Producers with `acks=all` can still write (2 ISR >= min ISR 2)
- Consumers continue reading from remaining replicas
- The failed broker catches up upon restart
