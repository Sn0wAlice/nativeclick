# État des lieux — compatibilité ClickHouse

Test du 2026-10-06 sur `nativeclick` (fork de `klickhouse`) 0.15.3 (commit `263cab6`), avec les features par défaut + `geo-types`, `tls`, `refinery` et `bb8` (`--all-features`).
Le protocole annoncé par le client est la révision **54448**. Les serveurs récents annoncent **54493**.

Rejouer le test : `docs/test-fork.sh [versions...]`. Les logs vont dans `docs/results/<version>.log`.
Image testée : `clickhouse/clickhouse-server:<tag>`, sur arm64. Les 13 tests d'intégration de `nativeclick/tests/main.rs` sont lancés avec `--test-threads=1`.

## Tableau

| Version CH | Type | Tests OK | État | Cause |
|---|---|---|---|---|
| 23.3 | LTS | 12/13 | 🟡 | `BFloat16` inconnu du serveur (le test le crée ; pas un bug du client) |
| 23.8 | LTS | 12/13 | 🟡 | idem |
| 24.3 | LTS | 12/13 | 🟡 | idem |
| 24.8 | LTS | 12/13 | 🟡 | idem |
| 25.3 | LTS | 13/13 | ✅ | |
| 25.8 | LTS | 13/13 | ✅ | version visée par la CI upstream (25.8.4) |
| 25.10 | stable | 13/13 | ✅ | |
| 25.12 | stable | 13/13 | ✅ | |
| 26.1 | stable | 13/13 | ✅ | |
| 26.2 | stable | 13/13 | ✅ | |
| 26.3 | LTS | 13/13 | ✅ | |
| 26.4 | stable | 13/13 | ✅ | |
| 26.5 | stable | 13/13 | ✅ | |
| 26.6 | stable | 13/13 | ✅ | |
| 26.7 | stable | 13/13 | ✅ | |
| 26.8 | stable | 13/13 | ✅ | |
| 26.9 | stable | 13/13 | ✅ | cassé avant le support ZSTD (1/13), voir ci-dessous |

## Ce qui cassait sur 26.9 (corrigé)

À partir de 26.9, la valeur par défaut du setting serveur `network_compression_method` passe de `LZ4` à `ZSTD` (niveau 3).
Ce changement est listé dans les *backward-incompatible changes* du changelog 26.9 ([ClickHouse#108786](https://github.com/ClickHouse/ClickHouse/pull/108786)).
Il ne dépend pas de la révision du protocole : même un client ancien reçoit des blocs ZSTD.

Ce qui se passait côté nativeclick avant le correctif :

- La compression est activée par défaut (feature `compression`, `CompressionMethod::default()` = LZ4, voir `nativeclick/src/protocol.rs`).
- Le serveur répond avec des blocs dont le marqueur de méthode vaut `0x90` (ZSTD). `nativeclick/src/compression.rs` n'accepte que `0x82` (LZ4) :
  ```
  ProtocolError("unexpected compression algorithm identifier: '90', expected 82 (LZ4)")
  ```
- La tâche de fond du client s'arrête et ferme la connexion. Côté appelant, cela donne `missing header block from server` ou un stream vide.
- Les INSERT ne sont pas touchés : le serveur détecte le codec de chaque bloc reçu.

C'est exactement le symptôme rencontré sur `tprm.mlab.sh`, qui a été épinglé sur `clickhouse-server:26.8` dans le commit `cd2202b` « fix clickhouse issue » : streams vides, connexion fermée après chaque requête, migrations rejouées.
Ce projet utilise `klickhouse 0.13.2`, qui a le même code de décompression, LZ4 uniquement.

## Contournements / correctifs

Le 2 est retenu et implémenté ; les autres restent pour mémoire.


1. **Côté serveur (validé)** : remettre LZ4 dans le profil utilisateur, par exemple avec un fichier `users.d/lz4.xml` :
   ```xml
   <clickhouse><profiles><default><network_compression_method>LZ4</network_compression_method></default></profiles></clickhouse>
   ```
   Avec ce fichier, 26.9.11.2 passe à 13/13.
2. **Côté client — ✅ fait** : `compression.rs` décode chaque bloc selon son octet de méthode (`0x02` aucune, `0x82` LZ4/LZ4HC, `0x90` ZSTD ; tout autre → erreur claire), via la dépendance `zstd` derrière la feature `compression`.
   Le client continue d'envoyer en LZ4 (le serveur accepte tout codec en entrée). La taille décompressée annoncée est maintenant plafonnée à 1 Gio et vérifiée.
   Test unitaire : `compression::tests::reads_every_server_codec`. Matrice relancée : 26.9 à 13/13 sans contournement.
3. **Côté client, minimal** : envoyer `network_compression_method='LZ4'` dans les settings de chaque requête.
   Aujourd'hui `internal_client_out.rs` envoie des settings vides, donc il faudrait implémenter l'envoi de settings.
4. **Palliatif** : désactiver la compression côté client (`default-features = false` sans `compression`). Non testé.

## Autres risques repérés (pas cassés aujourd'hui)

- **Types non parsés** par `Type::from_str` : `Date32`, `Nothing`, `Time`/`Time64`, `JSON`, `Dynamic`, `Variant`, `AggregateFunction`, et les tuples nommés `Tuple(a UInt8, …)`.
  Une requête qui renvoie un de ces types échoue, quelle que soit la version du serveur.
- **Révision utilisée pour le format** : le client choisit les champs selon `server_hello.revision_version` au lieu de `min(client, serveur)`.
  Sans effet tant que le client reste en 54448, mais ça cassera dès qu'on montera `DBMS_TCP_PROTOCOL_VERSION`.
- **Paquets Log** : `receive_log_data` est `unimplemented!()`. Le client panique si `send_logs_level` est activé dans un profil.
- **26.10+** : ClickHouse#113944 valide plus strictement les varints du handshake. Les valeurs actuelles sont correctes, mais c'est à surveiller.
- **Tests vs vieux serveurs** : `tests/test.rs` crée une colonne `BFloat16`, ce qui fait échouer les serveurs ≤ 24.8. C'est un problème du test, pas du client.
