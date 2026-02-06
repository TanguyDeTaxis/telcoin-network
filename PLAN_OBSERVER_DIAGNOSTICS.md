# Plan : Observer Health & Diagnostics

## Contexte

### Probleme
Les observers sur le devnet se bloquent regulierement. Steven (PR #534) :
> "I have high hopes this is going to fix some observer issues we are seeing in long running tests.
> Council members and personal observer nodes experiencing problems; validators more stable."

**Personne ne sait pourquoi les observers se bloquent** car il n'y a aucun outil de diagnostic.

### Travail deja fait (par TanguyDeTaxis)
| PR | Sujet | Statut |
|---|---|---|
| #534 | Channel no-op sans receiver (fix #457) | Approuve |
| #536 | `send_replace()` watch channels (fix #529) | Approuve |
| #539 | `tn_syncing`, `tn_epochInfo`, `tn_currentCommittee` RPC (fix #530) | En review |
| #524 | Suppression anciennes metriques Prometheus | Merge |

### Stack technologique metriques
- **ANCIEN** (supprime par PR #524) : `prometheus` crate, `IntGauge`, `IntCounter`, `Histogram`
- **NOUVEAU** (PR #487) : `tracing` + `tracing-opentelemetry` → export OTLP vers Jaeger/Grafana
- **Pattern a suivre** : `tracing::info!(target: "tn::metrics", metric_name = value)` + `#[instrument]`

### Issues ouvertes non traitees
| Issue | Sujet | Assigne | Code existant |
|---|---|---|---|
| #233 | Metrique retard blocks vs chain tip | sstanfield (rien fait) | ZERO - milestone "Alpha Mainnet Codefreeze" |
| #254 | Network/peer metrics dans libp2p | personne | ZERO |

---

## Validation industrie

Comparaison avec les protocoles majeurs - **toutes ces fonctionnalites sont standard** :

| Feature | Reth | Sui | Aptos | Solana | Cosmos | telcoin-network |
|---|---|---|---|---|---|---|
| Sync distance metric | `reth_sync_entities_*` | `highest_synced - highest_known` | `synced vs advertised` | `numSlotsBehind` | `catching_up` | **MANQUANT** |
| Health endpoint sync-aware | - | - | Node Health Checker | `GET /health` → "behind N" | `/status` | **Toujours 200 OK** |
| Peer count metrics | `reth_network_*` | Interne | `aptos_connections` | `getClusterNodes` | `/net_info` | **MANQUANT** |
| Sync error counters | Pipeline unwind | Checkpoint age | `error_label` on errors | Error codes | Byzantine gauge | **Log only, zero metrique** |
| Broadcast lag detection | N/A (staged pipeline) | Interne | Interne | Interne | Interne | **Type `Lagged` existe, JAMAIS log/compte** |

---

## Architecture observer - Points de blocage identifies

```
Gossip Network
     |
     v
[PrimaryNetworkHandle] ---> tx_last_published_consensus_num_hash (watch)
     |
     v
[spawn_track_recent_consensus] --- 4 workers fetch headers from peers
     |                                    |
     |                              POINT DE BLOCAGE 1:
     |                              Peers indisponibles = workers break
     |                              Aucune metrique de fetch failures
     |
     v
[spawn_stream_consensus_headers] --- catch_up_consensus_from_to()
     |                                    |
     |                              POINT DE BLOCAGE 2:
     |                              wait_for_execution() SANS TIMEOUT
     |                              Peut hang indefiniment
     |
     v
[consensus_output broadcast channel] (capacity: 100)
     |                                    |
     |                              POINT DE BLOCAGE 3:
     |                              Si consumer lent → TryRecvError::Lagged
     |                              Messages perdus SILENCIEUSEMENT
     |                              Zero log, zero compteur
     |
     v
[ExecutorEngine] --- execute blocks
     |
     v
[EVM Execution]
```

---

## Plan d'implementation

### Phase 1 : Metriques observer via OpenTelemetry (Issue #233 + #254)

**Scope** : Emettre des metriques structurees via `tracing` pour observer health.

#### 1a. Sync distance metric (Issue #233)

**Fichier** : `crates/state-sync/src/consensus.rs`
**Dans** : `spawn_track_recent_consensus()` et `spawn_stream_consensus_headers()`

Les donnees existent deja dans le `ConsensusBus` :
- `last_published_consensus_num_hash` = ce que le reseau dit etre le latest
- `latest_block_num_hash` = dernier block execute localement

```rust
// Pattern OpenTelemetry via tracing
let latest_network = consensus_bus.last_published_consensus_num_hash().borrow().number;
let latest_executed = consensus_bus.latest_block_num_hash().number;
let sync_distance = latest_network.saturating_sub(latest_executed);

tracing::info!(
    target: "tn::observer",
    sync_distance,
    latest_network,
    latest_executed,
    "observer sync status"
);
```

#### 1b. Node mode exposure

**Fichier** : `crates/consensus/executor/src/subscriber.rs`
**Dans** : `spawn()` et transitions de mode

```rust
let mode = consensus_bus.node_mode().borrow().clone();
tracing::info!(
    target: "tn::observer",
    node_mode = %mode,
    "node mode active"
);
```

#### 1c. Peer count metrics (Issue #254)

**Fichier** : `crates/network-libp2p/src/peers/manager.rs`
**Dans** : le peer manager event loop

```rust
tracing::info!(
    target: "tn::network",
    connected_peers = all_peers.len(),
    "peer count update"
);
```

#### 1d. State sync fetch metrics

**Fichier** : `crates/state-sync/src/consensus.rs`
**Dans** : `spawn_fetch_consensus()` - les workers qui fetch depuis les peers

```rust
// Sur succes
tracing::info!(
    target: "tn::observer",
    block_number = header.number,
    "consensus header fetched from peer"
);

// Sur echec
tracing::warn!(
    target: "tn::observer",
    block_number = number,
    "failed to fetch consensus header from peer"
);
```

---

### Phase 2 : Broadcast lag detection et logging

**Fichier principal** : `crates/consensus/executor/src/subscriber.rs`
**Fichier type** : `crates/types/src/sync.rs`

**Probleme actuel** : Quand le broadcast channel `consensus_output` (capacity 100) lag, `TryRecvError::Lagged` est retourne mais jamais traite.

**Solution** : Dans `follow_consensus()`, capter le lag et le signaler :

```rust
match rx_consensus_headers.recv().await {
    Ok(header) => self.handle_consensus_header(header).await?,
    Err(broadcast::error::RecvError::Lagged(n)) => {
        tracing::error!(
            target: "tn::observer",
            messages_lost = n,
            "broadcast channel lagged - observer lost consensus headers"
        );
        // Re-sync depuis les peers pour les headers manques
        // (trigger catch_up_consensus_from_to)
    }
    Err(broadcast::error::RecvError::Closed) => break,
}
```

---

### Phase 3 : Health endpoint sync-aware

**Fichier** : `crates/node/src/health.rs`

**Pattern inspire de Solana** : Le body HTTP indique l'etat reel.

```rust
// Au lieu de toujours repondre "OK":
let sync_distance = get_sync_distance(&consensus_bus);
let response = if sync_distance == 0 {
    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
} else {
    // "behind 42" format
    let body = format!("behind {sync_distance}");
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
};
```

**Note** : Le HTTP status reste 200 pour compatibilite GCP load balancer. Seul le body change.

Le `HealthcheckServer` a besoin d'un `Arc<ConsensusBus>` ou d'un `watch::Receiver` pour lire le sync distance.

---

### Phase 4 : Tests observer

#### Tests unitaires

| Test | Fichier | Ce qu'il verifie |
|---|---|---|
| `test_broadcast_lag_detection` | `crates/consensus/executor/src/tests/` | `RecvError::Lagged` est detecte et log |
| `test_sync_distance_calculation` | `crates/state-sync/src/tests/` | Delta correct entre network et local |
| `test_wait_for_execution_timeout` | `crates/consensus/primary/src/tests/` | `wait_for_execution()` ne hang pas forever |
| `test_healthcheck_reports_sync_status` | `crates/node/src/health.rs` | Body = "OK" quand sync, "behind N" sinon |

#### Tests e2e

| Test | Fichier | Ce qu'il verifie |
|---|---|---|
| `test_observer_late_join_catchup` | `crates/e2e-tests/tests/it/` | Observer demarre apres N blocks, rattrape |
| `test_observer_reconnect_after_partition` | `crates/e2e-tests/tests/it/` | SIGSTOP/SIGCONT → observer recover |
| `test_observer_epoch_boundary_sync` | `crates/e2e-tests/tests/it/` | Observer sync a travers changements d'epoque |
| `test_observer_soak_20_epochs` | `crates/e2e-tests/tests/it/` | 20 epochs, drift < N blocks en continu |

#### Test de broadcast lag (pattern complet)

```rust
#[tokio::test]
async fn test_broadcast_lag_is_detected() {
    let (tx, _rx) = broadcast::channel::<ConsensusHeader>(100);
    let mut slow_rx = tx.subscribe();

    // Remplir au-dela de la capacite
    for i in 0..150 {
        let _ = tx.send(mock_header(i));
    }

    // Le slow consumer doit detecter le lag
    match slow_rx.try_recv() {
        Err(broadcast::error::TryRecvError::Lagged(n)) => {
            assert_eq!(n, 50, "should have lost 50 messages");
        }
        other => panic!("Expected Lagged, got {other:?}"),
    }
}
```

#### Test observer late-join (pattern complet)

```rust
#[test]
#[ignore = "e2e test"]
fn test_observer_late_join_catchup() -> eyre::Result<()> {
    // 1. Demarrer 4 validators SANS observer
    // 2. Envoyer 5 transactions, attendre confirmation
    // 3. Recorder la hauteur des validators
    // 4. MAINTENANT demarrer l'observer
    // 5. Attendre que l'observer rattrape (max 120s)
    // 6. Verifier que les block hashes correspondent
    Ok(())
}
```

---

## Ordre de priorite recommande

### Option A : Scope complet (4 PRs)

| PR | Contenu | Effort estime | Impact |
|---|---|---|---|
| **PR 1** | Phase 1 (metriques OTel) + Phase 2 (broadcast lag) | Moyen | Tres eleve - visibilite immediate |
| **PR 2** | Phase 3 (health endpoint) | Petit | Eleve - diagnostics operationnels |
| **PR 3** | Phase 4 tests unitaires | Moyen | Eleve - prevention regressions |
| **PR 4** | Phase 4 tests e2e | Grand | Tres eleve - detection bugs long-running |

### Option B : Scope reduit (1 PR concentre)

Si le scope complet est trop gros, commencer par **PR 1 seul** :
- Sync distance metric (Issue #233 - milestone mainnet)
- Broadcast lag detection + logging
- State sync fetch failure logging
- Tests unitaires associes

C'est le **minimum impactant** : ca donne immediatement a Steven et Grant la visibilite sur pourquoi les observers se bloquent.

---

## Fichiers a modifier (resume)

| Fichier | Phase | Modification |
|---|---|---|
| `crates/state-sync/src/consensus.rs` | 1a, 1d | Sync distance + fetch metrics |
| `crates/state-sync/src/lib.rs` | 1a | Sync distance dans catch_up |
| `crates/consensus/executor/src/subscriber.rs` | 1b, 2 | Node mode + broadcast lag |
| `crates/network-libp2p/src/peers/manager.rs` | 1c | Peer count metrics |
| `crates/node/src/health.rs` | 3 | Sync-aware health |
| `crates/e2e-tests/tests/it/` | 4 | Nouveaux tests observer |
| `crates/state-sync/src/tests/` (nouveau) | 4 | Tests unitaires sync |

---

## References

- Issue #233 : https://github.com/Telcoin-Association/telcoin-network/issues/233
- Issue #254 : https://github.com/Telcoin-Association/telcoin-network/issues/254
- Issue #457 : https://github.com/Telcoin-Association/telcoin-network/issues/457 (fait - PR #534)
- Issue #529 : https://github.com/Telcoin-Association/telcoin-network/issues/529 (fait - PR #536)
- Issue #530 : https://github.com/Telcoin-Association/telcoin-network/issues/530 (fait - PR #539)
- PR #487 : OpenTelemetry initial hookup (sstanfield)
- PR #524 : Suppression anciennes metriques Prometheus (TanguyDeTaxis)
- Solana `GET /health` pattern : https://solana.com/docs/rpc/http/gethealth
- Sui fullnode metrics : https://forums.sui.io/t/key-metrics-for-fullnode/17333
- Aptos important metrics : https://aptos.dev/network/nodes/measure/important-metrics
