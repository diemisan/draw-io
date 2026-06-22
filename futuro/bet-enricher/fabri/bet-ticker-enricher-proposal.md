# Bet Ticker / Bet History – Propuesta consolidada (Data Model + Flink Architecture)

> Documento consolidado a partir de:
> - `bet-ticker-architecture-proposal-main.md` (decisiones de arquitectura Flink)
> - PDFs de eventos: `BetCaptured`, `BetAccepted`, `BetPlaced`, `BetFailed`, `BetRejected`, `BetRollback`, `LegGraded`, `BetGraded`, `BetSettled`, `BetSettlementFailed`
> - PDFs `History_ Data Modeling` y `History_ Data Integration`
> - Diagrama UML/ER (`image-20260507-140132.png`)

---

## 1. Objetivo

Construir un pipeline en **Apache Flink** que actúa como **bet enricher**: consume los eventos del ciclo de vida de una bet emitidos por el dominio (Bet Placement + Bet Settlement), los enriquece con los datos dimensionales que faltan en el evento original (sport, tournament, fixture, market, competitors, punter) y emite un **EnrichedBet** consolidado (snapshot completo, JSON único — sección 4.3.1 de `History_ Data Modeling`) a:

- **StarRocks** (serving layer del Bet Ticker, primary key model con upsert por `betReference`).
- **Pulsar topic** `enriched-bets` (changelog para consumers downstream: Splunk, analytics, etc.).
- **DLQs** para enrichment fallido, transiciones inválidas y gaps de secuencia.

El job procesa de manera unificada todos los eventos de placement y settlement; la rehidratación de la proyección para eventos de settlement diferidos (minutos / días después del placement) se resuelve mediante un lookup lazy a StarRocks cuando el keyed state ya no contiene la bet (state evicted por TTL, restart del job, etc.). No hay perfiles ni modos de despliegue: una sola pipeline cubre placement y settlement.

---

## 2. Eventos de entrada (resumen consolidado)

Los eventos viajan en topics Pulsar como CloudEvents v1.0 con headers `ce-type`, `ce-source`, `ce-id`, `ce-time`, `ce-correlationid`, `ce-traceparent`. La nomenclatura `ce-type` sigue la convención `sportsbook.betting.{placement|settlement}.{entity}.{action}.v1`.

### 2.1 Bet Placement events

| Evento | `ce-type` | Identificadores principales | Carga útil clave |
|---|---|---|---|
| `BetCaptured` | `sportsbook.betting.betplacement.bet.captured.v1` | `betslipId`, `punterId`, `bet.betReference` | `acceptOddsChangesMode`, `bet.{stake,currency,punterOdds,toWin,potentialPayout,legs[].selections[].id}`, `metadata{channel,ip,ua,fp,referer,receivedAt}` |
| `BetAccepted` | `sportsbook.betting.betplacement.bet.accepted.v1` | `betslipId`, `punterId`, `bet.reference` | `bet.{punterOdds,systemOdds,acceptedOdds}`, `legs[].selections[].{selectionId,fixtureId,marketId,isLive,...Odds}`, `validationTrail[]`, `validationDurationMs` |
| `BetPlaced` | `sportsbook.betting.betplacement.bet.placed.v1` | `betslipId`, `punterId`, `ticketId`, `bet.betId`, `bet.reference` | `bet.{placedAt, ...,legs[].legId,selections[]}`, `wallet{transactionId,transactedAt,previousBalance,updatedBalance}`, `liveDelay` |
| `BetFailed` | `sportsbook.betting.betplacement.bet.failed.v1` | `betslipId`, `bet.reference` | `bet.{type,lastStatus,failedAt}`, `failure{code,message}`, `recoveryAction` (códigos `BPS-ERR-9XX`) |
| `BetRejected` | `sportsbook.betting.betplacement.bet.rejected.v1` | `betslipId`, `bet.reference` | `bet.{rejectionCode,rejectionMessage,rejectedAt}`, `validationTrail[]` (códigos `BPS-ERR-1XX..5XX`) |
| `BetRollback` | `sportsbook.betting.betplacement.bet.rollback.v1` | `betslipId`, `ticketId`, `bet.reference` | `bet.statusBeforeRollback`, `rollback{rollbackId,reason,walletRefunded,refundTransactionId}`, `timing`, `failure` |

### 2.2 Bet Settlement events (Topic: `persistent://sportsbook/betting/settlement-events`)

| Evento | `ce-type` | Identificadores principales | Carga útil clave |
|---|---|---|---|
| `LegGraded` | `sportsbook.betting.betsettlement.leg.graded.v1` | `ticketId`, `betId`, `betReference`, `legId` | `legType`, `outcome` (GradingResult), `reason`, `gradedBy` |
| `BetGraded` | `sportsbook.betting.betsettlement.bet.graded.v1` | `ticketId`, `betId`, `betReference` | `outcome` (GradingResult global), `gradedBy`, `legs[].{legId,outcome,gradedAt,reason}` |
| `BetSettled` | `sportsbook.betting.betsettlement.bet.settled.v1` | `ticketId`, `betId`, `betReference`, `settlementId`, `previousSettlementId` | `outcome` (SettlementResult), `settledBy`, `wallet{punterId,transactionId,operation,amount{value,currency}}` |
| `BetSettlementFailed` | `sportsbook.betting.betsettlement.bet.settlement-failed.v1` | `ticketId`, `betId`, `betReference`, `settlementId` | `outcome` (intentado), `failureCode`, `failureReason` |

### 2.3 Enumeraciones derivadas

- **BetStatus**: `Requested`, `Accepted`, `Placed`, `Rejected`, `Failed`, `Settled` (image y modelo lógico).
- **BetGradeStatus / GradingResult**: `Pending` (0), `Cancelled` (1), `Won` (2), `Lost` (3), `Void` (4), `Push` (5) — completar con docs faltantes.
- **LegGradeStatus / GradingResult**: idem `GradingResult`.
- **SettlementResult**: `Pending` (0), `Won` (1), `Lost` (2), `Void` (3) — la spec usa enteros (`outcome`) sin enumerar todos los valores.
- **BetType**: `Straight`, `Parlay` (`SGP` aparece como `LegType` en `CapturedLeg`).
- **AcceptOddsChangesMode**: `Any`, `Higher`, `None`.

### 2.4 Tabla de transición consolidada (evento → estado)

Derivada del UML/PNG y los PDFs:

| Evento | BetStatus tras evento | BetGradeStatus | LegGradeStatus | Notas |
|---|---|---|---|---|
| `BetCaptured` | `Requested` | – | – | Crea el EnrichedBet con `metadata.enrichment=PENDING` |
| `BetAccepted` | `Accepted` | – | – | Añade `acceptedOdds`, `systemOdds`, validation trail |
| `BetPlaced` | `Placed` | – | – | Asigna `ticketId`, `bet.id` (`betId`), `bet.legs[].legId`; wallet debit ok |
| `BetFailed` | `Failed` | – | – | Falla técnica (`BPS-ERR-9XX`); guarda `failure` y `lastStatus` |
| `BetRejected` | `Rejected` | – | – | Falla negocio (`BPS-ERR-1XX..5XX`); guarda `rejection` |
| `BetRollback` | `Failed` | – | – | Stake refundido tras wallet debit |
| `LegGraded` | (sin cambio) | – | leg `gradeStatus = outcome` | Solo actualiza el leg específico |
| `BetGraded` | (sin cambio o `Settled` según el modelo de salida) | `gradeStatus = outcome` | (todos los legs vienen con outcome) | Transición Pending → resultado final |
| `BetSettled` | `Settled` | – | – | Wallet settle ok; `settlement.{result,walletOperation,walletAmountValue,walletCurrency,walletTransactionId,settledBy}` |
| `BetSettlementFailed` | (sin cambio) | – | – | Side-effect: cuenta de reintentos, alerta operacional |

> Notar: la tabla en el PNG indica que `BetGraded` setea `bet.gradeStatus = outcome` pero **no** marca aún `BetStatus=Settled` (eso lo hace `BetSettled`). Hay ambigüedad en el modelo: en el JSON de ejemplo (`History_ Data Modeling`) un bet `Placed` con `gradeStatusName=Pending` se considera "abierto". Cuando llega `BetGraded` el `gradeStatusId/Name` cambia; con `BetSettled` el `betStatus` pasa a `Settled` y se completa el bloque `settlement`. Lo modelamos así para evitar perder información intermedia.

---

## 3. Identidad y correlación

La clave estable a lo largo del ciclo de vida es **`bet.reference` (UUID)** — aparece en *todos* los eventos. `betId` y `ticketId` se asignan en `BetPlaced` y no existen en `BetCaptured/Accepted/Rejected/Failed`.

**Decisión**: usar `betReference` como key de partición en Flink (`keyBy(betReference)`).

`ticketId` y `betId` se ponen en la proyección como atributos cuando se reciben. El **primary key** del sink (StarRocks) será **`ticketId`** cuando exista; mientras no exista (estado `Requested`/`Accepted`/`Rejected`/`Failed` pre-BetPlaced), se usa `betReference`. Para evitar ambigüedad, el modelo físico se diseña con `ticketId` nullable y un **índice único compuesto** `(operatorId, betReference)`; el upsert lo hace Flink sink contra la PK.

Alternativa más simple: usar `betReference` como PK del registro de StarRocks de manera permanente, y dejar `ticketId` como atributo. **Recomendado**.

### Orden y secuenciación

Los eventos no traen `sequenceNumber` explícito en los esquemas actuales (a diferencia de la propuesta original). Para idempotencia y detección de gaps se construye un **sequence sintético** por bet:

```
seq(event) = (eventTypeOrder, ce-time)
```

donde `eventTypeOrder` se deriva de la posición lógica:

```
Captured(0) < Accepted(1) < {Rejected(2a)|Failed(2b)|Placed(2c)} < Rollback(3) <
LegGraded(4)* < BetGraded(5) < BetSettled(6) | BetSettlementFailed(6')
```

Idempotencia se garantiza por:
- Dedup por `ce-id` (CloudEvents id) en el operador de ingesta.
- En el projector: el estado solo aplica una transición válida y descarta eventos cuyo `(type, ce-time)` ya fue aplicado.

> **DATO FALTANTE / Confirmar con producer**: si los eventos pueden re-emitirse con un `ce-id` nuevo (reentrega tras crash), necesitamos también un campo `sequenceNumber` monotónico por `betReference` o equivalente. Ver §10 Datos faltantes.

---

## 4. Qué necesita enrichment — campos derivados de la sección 4.3.1

> Esta sección recorre el JSON de `EnrichedBet` (sección 4.3.1 del documento de Data Modeling) y clasifica cada bloque en una de tres categorías:
> 1. **Directo desde evento** — copia desde alguno de los eventos de entrada, sin enrichment.
> 2. **Enrichment global** — viene de un catálogo pequeño/estable replicado a todas las subtareas vía broadcast state.
> 3. **Enrichment específico** — viene de un catálogo grande / alta cardinalidad consultado on-demand (Caffeine local + StarRocks como fallback).

### 4.1 Campos sin enrichment (extraídos del evento)

| Bloque / campo | Evento(s) fuente |
|---|---|
| `ticketId` | `BetPlaced.ticketId` (y subsecuentes settlement events) |
| `betslipId` | `BetCaptured.betslipId` y siguientes |
| `operatorId`, `brandId`, `offeringId` | `BetCaptured` |
| `acceptOddsChangesMode` | `BetCaptured` |
| `clientLanguage`, `clientLocale`, `clientOddsFormat`, `clientCurrency` | `BetCaptured` (versión ECST) |
| `clientDisplayedStake`, `clientDisplayedToWin`, `clientDisplayedPayout` | `BetCaptured` (versión ECST) |
| `metadata.{srcEventId, srcEventType, srcTime, enrichment, retry, version}` | Construido por el pipeline a partir de cada evento aplicado |
| `punter.id` | `BetCaptured.punterId` |
| `capturedMetadata.{channel, ipAddress, userAgent, fingerprint, referer, receivedAt}` | `BetCaptured.metadata` |
| `bet.{reference, type, stake, currency, toWin, potentialPayout}` | `BetCaptured` / `BetAccepted` / `BetPlaced` |
| `bet.id` | `BetPlaced.bet.betId` |
| `bet.placedAt` | `BetPlaced.bet.placedAt` |
| `bet.acceptedOdds` (ComputedOdds) | `BetAccepted` / `BetPlaced` |
| `bet.legs[].id`, `bet.legs[].type`, `bet.legs[].position` | `BetPlaced.bet.legs[]` |
| `bet.legs[].acceptedOdds` | `BetAccepted`/`BetPlaced` |
| `bet.legs[].selections[].id` (selectionId) | Todos los eventos de placement (en `BetCaptured` viene como string, en `BetAccepted`/`BetPlaced` como `selectionId`) |
| `bet.legs[].selections[].acceptedOdds` (con `oddsVersion`) | `BetAccepted` / `BetPlaced` (VersionedOdds) |
| `bet.legs[].gradeStatus`, `gradedAt`, `gradedBy` | `LegGraded` (matcheado por `legId`) |
| `bet.gradeStatusId`, `bet.gradeStatusName` | `BetGraded.outcome` (mapeado al enum `GradingResult`) |
| `bet.betStatus` | Derivado del tipo de evento aplicado |
| `bet.walletDebit.{transactionId, previousBalance, updatedBalance}` | `BetPlaced.wallet` |
| `bet.settlement.{result, settledBy, walletTransactionId, walletOperation, walletAmountValue, walletCurrency}` | `BetSettled.{outcome, settledBy, wallet}` |
| `bet.rejection.{code, message, rejectedAt}` | `BetRejected.bet.{rejectionCode, rejectionMessage, rejectedAt}` |
| (Interno, no en JSON final) `failure`, `rollback`, `settlementFailure` | `BetFailed`, `BetRollback`, `BetSettlementFailed` — se conservan para auditoría / DLQ |

### 4.2 Enrichment **GLOBAL** (broadcast state con versionado temporal)

Todas las dimensiones de oferta y catálogo se distribuyen vía broadcast state, con historial temporal por entidad usando `List<Version>` mantenida **ordenada ascendente por `validFromMs`** y lookup vía búsqueda binaria de la entrada con mayor `validFromMs <= eventTs` (equivalente a `floorEntry` de `TreeMap`). Esto garantiza **corrección temporal**: la bet ve la versión de la dimensión vigente al momento del placement original, aunque luego cambie.

> **¿Por qué `List` y no `TreeMap`?** Flink no tiene serializer nativo para `TreeMap` / `SortedMap` (ver Flink docs sobre tipos soportados: solo `Map`, `List`, `Set`, `Collection` como interfaces). Un campo `TreeMap` en un POJO o como valor de `MapStateDescriptor` cae a Kryo, lo cual rompe `disableGenericTypes()` y la compatibilidad de schema evolution con savepoints. La alternativa es una `List<Version>` que se mantiene ordenada por inserción (insertion sort sobre estructura pequeña) o `Map<Long, Version>` con búsqueda lineal por `validFromMs` (aceptable porque cada entidad típicamente tiene 1–20 versiones).

| Dimensión | Campos del EnrichedBet que se enriquecen | Clave de lookup | Topic / fuente | Notas |
|---|---|---|---|---|
| **Sport** | `bet.legs[].selections[].sportId`, `sportName` | `sportId` (resuelto desde `Fixture.sportId`) | `catalog.sports` (Pulsar compactado) | Cambio rarísimo; basta versión actual |
| **Tournament** | `bet.legs[].selections[].tournamentId`, `tournamentName` | `tournamentId` (desde `Fixture.tournamentId`) | `catalog.tournaments` (Pulsar compactado) | Cambio lento; versionado por nombre/temporada |
| **Market Definition** | template de market (`defId`, `type`, `name`, `clientName`) | `marketDefId` (desde `Market.marketDefId`) | `catalog.market-definitions` | Estable, referenciado por instancias de market |
| **Fixture** | `bet.legs[].selections[].fixture.{id, type, name, date, phase, status, timeFrame, clientName, clientDate}` | `fixtureId` (desde `BetAccepted/BetPlaced.selections[].fixtureId`) | `catalog.fixtures` (Pulsar compactado, `Match`/`Race`/`Outright`) | **Versionado**: phase/status cambian durante el partido — lookup por `floor(validFromMs <= ce-time)` sobre la `List` ordenada |
| **Market (instance)** | `bet.legs[].selections[].market.{id, status, name, clientName, selectionStatus, selectionName, clientSelectionName, odds, clientOdds}` | `marketId` (desde `BetAccepted/BetPlaced.selections[].marketId`) | `catalog.markets` (Pulsar compactado) | **Versionado**: status/odds cambian frecuentemente — lookup por `floor(validFromMs <= ce-time)` sobre la `List` ordenada |
| **Competitor** | `bet.legs[].selections[].fixture.competitors[].{id, name, clientName, position}` | `competitorId` (desde `Fixture.homeCompetitorId`, `awayCompetitorId`, `competitorIds[]`) | `catalog.competitors` (Pulsar compactado) | Cambio raro; versionado por nombre/equipo |
| **Operator / Brand / Offering** | `operatorName`, `brandName`, `offeringName` (pendiente confirmar) | `operatorId`, `brandId`, `offeringId` | `catalog.operators` / `brands` / `offerings` | Tiny set |

**Patrón uniforme**: cada catálogo se modela como `MapStateDescriptor<EntityId, List<EntityVersion>>` en broadcast state, manteniendo la lista ordenada ascendente por `validFromMs`. Un helper `VersionLookup.floor(list, eventCeTimeMs, *Version::getValidFromMs)` ejecuta una búsqueda binaria que devuelve la versión con mayor `validFromMs <= eventCeTimeMs`, equivalente a `TreeMap.floorEntry`. Ver §6.4 para la definición del helper y la regla de serialización.

**Lazy loading desde StarRocks**: si el broadcast state perdió una versión por TTL (entries muy viejas), o si el catálogo Pulsar nunca emitió esa versión a este job (p.ej. job nuevo arrancado tras una bet vieja), el enricher consulta JDBC contra las tablas `offer.*_history` de StarRocks como fallback. El resultado se inyecta en el broadcast state local de la subtarea (no se broadcastea de vuelta) y se procede con el enrichment.

### 4.3 Enrichment **ESPECÍFICO** (keyed lookup contra Pulsar CDC)

Sólo entidades de muy alta cardinalidad o asociadas a una bet individual:

| Dimensión | Campos del EnrichedBet que se enriquecen | Clave de lookup | Topic / fuente | Notas |
|---|---|---|---|---|
| **Punter** | `punter.{id, limits, …}` (limits y otros atributos a confirmar — §10) | `punterId` (desde `BetCaptured.punterId`) | `punter.profile-cdc` (Pulsar CDC) | Alta cardinalidad; se mantiene como keyed state (TTL configurable) en una segunda key dimension o como dim consultada al primer evento |
| **EnrichedBet previo** (rehidratación de state) | Todo el snapshot de una bet vieja | `betReference` | — | El Bet Status Manager carga snapshot previo desde StarRocks cuando state está vacío (TTL evicted o restart) |

> La rehidratación no es enrichment per se sino reconstrucción de state del projector. La diferenciamos porque cruza el límite del job hacia StarRocks (read-only).

### 4.4 Resumen: ¿qué se hace en qué momento del ciclo de vida?

| Evento entrante | Enrichment global a aplicar | Enrichment específico a aplicar | Rehidratación previa requerida |
|---|---|---|---|
| `BetCaptured` | – (la versión ECST trae `clientSportName`/`clientCategoryName`/`clientTournamentName`/`clientFixtureName` inline) | – (no hay `fixtureId`/`marketId` aún) | No (crea documento) |
| `BetAccepted` | Sport (vía Fixture.sportId), Tournament, Market Definition | Fixture, Market, Competitor, Punter | No (suele venir poco después de Captured) |
| `BetPlaced` | Idem BetAccepted (refresca si BetAccepted fue parcial) | Idem | No |
| `BetRejected` | – | – | Sí (si BetCaptured no había arrancado state) |
| `BetFailed` | – | – | Sí (si state vacío) |
| `BetRollback` | – | – | Sí |
| `LegGraded` | – | – | **Sí** (típico: días después; state evicted) |
| `BetGraded` | – | – | **Sí** |
| `BetSettled` | – | – | **Sí** |
| `BetSettlementFailed` | – | – | **Sí** (para registrar el contador en metadata) |

Conclusión: el enrichment "real" (sport/tournament/market-def + fixture/market/competitor) se ejecuta **sólo en BetAccepted y BetPlaced**. Para el resto de eventos basta con tener el snapshot de la bet (en state vivo o rehidratado desde StarRocks) y mutarlo.

---

## 5. Modelo de datos – EnrichedBet (salida)

### 5.1 Concepto

El **EnrichedBet** representa el snapshot completo de la bet en cualquier momento de su ciclo de vida. Es la única salida del pipeline y se emite **cada vez que se aplica un evento** (snapshot completo, no delta). Esto simplifica:

- Sink (publicación a Pulsar simple por `betReference`).
- Recuperación ante eventos perdidos en el sink.
- Consumo por Bet Ticker UI (Splunk en tiempo real, projecta el último estado por bet).
- Consumo por StarRocks (archiva el topic como histórico, sirve queries históricas a la UI vía JDBC add-on).

### 5.2 Estructura JSON (alineada con `History_ Data Modeling` §4.3.1)

```jsonc
{
  "ticketId":      "019cd7bf-dcfa-7c84-b8f9-aa1dbde54866",
  "betslipId":     "019bff29-733c-7663-9487-08a21969df2b",
  "operatorId":    1,
  "brandId":       1,
  "offeringId":    1,

  "acceptOddsChangesMode":  "Any",
  "clientLanguage":         "en-US",
  "clientLocale":           "en-US",
  "clientOddsFormat":       "AMERICAN",
  "clientDisplayedStake":   "50.00",
  "clientDisplayedToWin":   "75.00",
  "clientDisplayedPayout":  "125.00",
  "clientCurrency":         "USD",

  "metadata": {
    "srcEventId":   "<ce-id del último evento aplicado>",
    "srcEventType": "sportsbook.betting.betplacement.bet.placed.v1",
    "srcTime":      "2026-03-10T12:36:41.336Z",
    "enrichment":   "SUCCESS | PARTIAL | FAILED",
    "retry":        2,
    "version":      1
  },

  "punter": {
    "id":     "B85423",
    "limits": { /* lookup dim a confirmar */ }
  },

  "capturedMetadata": {
    "channel":     "web",
    "ipAddress":   "172.65.89.224",
    "userAgent":   "Mozilla/5.0 ...",
    "fingerprint": "t13d1516h2_...",
    "referer":     "https://...",
    "receivedAt":  "2026-03-10T12:36:40.123Z"
  },

  "bet": {
    "reference":          "a31cf868-fb10-42c8-92c0-40f8901937dd",
    "id":                 "019cd7bf-dcac-74a3-be42-b3eb18e045f8",
    "type":               "Straight",
    "placedAt":           "2026-03-10T12:36:41.336Z",
    "stake":              25,
    "currency":           "USD",
    "acceptedOdds":       { "decimal": 1.9, "american": -111, "fractional": { "numerator": 9, "denominator": 10 } },
    "toWin":              22.50,
    "potentialPayout":    47.50,

    "legs": [
      {
        "id":           "019cd7bf-dcac-77eb-9bb9-65fa38f83073",
        "type":         "Simple",
        "position":     0,
        "acceptedOdds": { /* idem */ },
        "gradeStatus":  "Pending | Cancelled | Won | Lost | Void | Push",
        "gradedAt":     null,
        "gradedBy":     null,

        "selections": [
          {
            "id":            "<selectionId del evento>",
            "sportId":       "soccer-id",
            "sportName":     "Soccer",
            "tournamentId":  "<...>",
            "tournamentName":"<...>",

            "fixture": {
              "id":          "ft|mtch|1234-01",
              "type":        "MATCH",          // MATCH | RACE | OUTRIGHT
              "name":        "Brazil vs Argentina",
              "date":        "2026-06-15T18:00:00Z",
              "phase":       "PREMATCH | IN_PLAY | CONCLUDED",
              "status":      "NOT_STARTED | STARTED | SUSPENDED | ENDED | CANCELED",
              "timeFrame":   "FULL_TIME | OVER_TIME | FT_OT | PERIOD",
              "clientName":  "...",
              "clientDate":  "...",
              "competitors": [
                { "id":"...", "name":"Brazil",    "clientName":"Brazil",    "position":"Home" },
                { "id":"...", "name":"Argentina", "clientName":"Argentina", "position":"Away" }
              ]
            },

            "market": {
              "id":                 "ft|mkt|01-a07-01",
              "defId":              "ft|mktdef|01-a07",
              "type":               "MONEYLINE_3WAY | ASIAN_UNDER_OVER | EUROPEAN_HANDICAP | ...",
              "status":             "OPEN | SUSPENDED | CLOSED",
              "name":               "Match Winner",
              "clientName":         "Match Winner",
              "selectionStatus":    "OPEN | SUSPENDED | CLOSED",
              "selectionName":      "Brazil",
              "clientSelectionName":"Brazil",
              "odds":       { "version":"2a", "decimal":2.5,  "american":150, "fractional":{...}, "capturedAt":"..." },
              "clientOdds": { "version":"2a", "decimal":2.5,  "american":150, "fractional":{...}, "capturedAt":"..." }
            },
            "acceptedOdds": { "version":"d290...","decimal":1.9,"american":-111,"fractional":{...} }
          }
        ]
      }
    ],

    "betStatus":       "Requested | Accepted | Placed | Rejected | Failed | Settled",
    "gradeStatusId":   0,
    "gradeStatusName": "Pending",

    "walletDebit": {
      "transactionId":   "txn-uuid",
      "previousBalance": 500,
      "updatedBalance":  450
    },

    "settlement": {
      "result":              1,
      "settledBy":           "System",
      "walletTransactionId": "c3b37ed9-...",
      "walletOperation":     "SettleWon | SettleLost | SettleVoid",
      "walletAmountValue":   2.00,
      "walletCurrency":      "USD"
    },

    "rejection": {
      "code":       "BPS-ERR-105",
      "message":    "Selection is suspended",
      "rejectedAt": "2026-03-10T12:36:41.336Z"
    }
  }
}
```

### 5.3 Reglas de poblamiento por evento

| Evento entrante | Campos seteados / actualizados |
|---|---|
| `BetCaptured` | Crea documento. `betslipId`, `punterId`, `operatorId`, `brandId`, `offeringId`, `acceptOddsChangesMode`, `clientDisplayed*`, `capturedMetadata`, `bet.{reference,type,stake,currency,toWin,potentialPayout,legs[].selections[].id}`, `bet.betStatus="Requested"`, `metadata.srcEventType`, `metadata.srcTime` |
| `BetAccepted` | `bet.acceptedOdds`, `bet.legs[].acceptedOdds`, `bet.legs[].selections[].acceptedOdds`, `bet.betStatus="Accepted"`, anexa `validationTrail` a metadata interna (no necesariamente en JSON final) |
| `BetPlaced` | `ticketId`, `bet.id`, `bet.placedAt`, `bet.legs[].id`, `bet.walletDebit`, `bet.betStatus="Placed"`, `metadata.enrichment="SUCCESS"` (si todos los lookups pasaron) |
| `BetRejected` | `bet.betStatus="Rejected"`, `bet.rejection.{code,message,rejectedAt}` |
| `BetFailed` | `bet.betStatus="Failed"`, anexa `failure.{code,message}` a metadata, `recoveryAction` |
| `BetRollback` | `bet.betStatus="Failed"`, `bet.rollback.{id,reason,walletRefunded,refundTransactionId}` (campo nuevo) |
| `LegGraded` | `bet.legs[legId].{gradeStatus, gradedAt, gradedBy}` |
| `BetGraded` | `bet.gradeStatusId`, `bet.gradeStatusName`; refresca todos los legs con outcome incluido |
| `BetSettled` | `bet.betStatus="Settled"`, `bet.settlement.*`, `settlementId`, `previousSettlementId` si aplica |
| `BetSettlementFailed` | Incrementa contador interno, emite alerta. **No** modifica `betStatus`. |

### 5.4 Modelo físico (StarRocks)

Primary Key table con upsert nativo:

```sql
CREATE TABLE betticker_enriched_bets (
  bet_reference        VARCHAR(64)  NOT NULL,
  ticket_id            VARCHAR(64),
  betslip_id           VARCHAR(64),
  operator_id          INT,
  brand_id             INT,
  offering_id          INT,
  punter_id            VARCHAR(64),
  bet_status           VARCHAR(20),
  grade_status_id      TINYINT,
  grade_status_name    VARCHAR(20),
  bet_type             VARCHAR(20),
  placed_at            DATETIME,
  last_event_type      VARCHAR(80),
  last_event_time      DATETIME,
  enrichment           VARCHAR(10),       -- SUCCESS | PARTIAL | FAILED
  retry_count          SMALLINT,
  payload              JSON,              -- EnrichedBet completo
  -- columnas materializadas para filtros del ticker:
  sport_ids            ARRAY<VARCHAR(40)>,
  fixture_ids          ARRAY<VARCHAR(64)>,
  market_ids           ARRAY<VARCHAR(64)>,
  stake                DECIMAL(18,4),
  currency             VARCHAR(3),
  potential_payout     DECIMAL(18,4),
  actual_payout        DECIMAL(18,4)
) PRIMARY KEY(bet_reference)
DISTRIBUTED BY HASH(bet_reference) BUCKETS 32
PROPERTIES ("enable_persistent_index" = "true");
```

> **Trade-off**: `bet_reference` como PK simplifica el upsert mientras `ticket_id` está nullo. Si el equipo de Data prefiere `ticket_id` como PK por trazabilidad operacional, se puede invertir, pero se necesita una tabla auxiliar `betslip→ticket` durante el periodo `Requested/Accepted/Rejected/Failed`.

---

## 6. Arquitectura Flink

> **Plataforma**: Apache Flink **1.18** (versión compatible con el Pulsar-Flink connector). Esto impone restricciones en los modelos serializados que se detallan en §6.4.

El job se descompone en **4 capas** con responsabilidades aisladas. El orden adoptado es **Project → Enrich**: el state machine se aplica primero sobre los eventos crudos, y el enrichment ocurre lazy una sola vez por bet cuando aparecen referencias dimensionales (típicamente en `BetAccepted`).

| Capa | Nombre | Responsabilidad | Tipo de operador | Modelo de entrada | Modelo de salida |
|---|---|---|---|---|---|
| 1 | **Ingestion** | Consumir Pulsar (eventos de bet + catálogos), canonicalizar a POJOs | `PulsarSource` + `MapFunction` | CloudEvent JSON | `BetEvent` / `CatalogUpdate` |
| 2 | **Bet Status Manager** | Event sourcing (eventLog por bet), state machine, replay de late events, rehidratación lazy, TTL/limpieza terminal | `KeyedProcessFunction` (key = `betReference`) | `BetEvent` | `BetProjection` |
| 3 | **Bet Enricher** | Resolver dimensiones globales versionadas (búsqueda binaria sobre `List<Version>` ordenada) sobre broadcast state + lookup específico para punter | `KeyedBroadcastProcessFunction` (key = `betReference`, broadcast = catalogs) | `BetProjection` + `CatalogUpdate` | `EnrichedBet` |
| 4 | **Sink** | Publica a Pulsar (`enriched-bets` + DLQs); consumidores downstream (StarRocks, Splunk, etc.) se subscriben al topic | `PulsarSink` × 4 | `EnrichedBet` | — |

> **Notas de diseño clave**:
> - **No hay watermarks ni dedupe en Capa 1**. El event sourcing en Capa 2 (eventLog completo + replay) maneja por sí mismo el orden de eventos, late arrivals y duplicados — el dedupe efectivo es por `(betReference, sequenceNumber)` dentro del eventLog. Capa 1 sólo deserializa y canonicaliza.
> - **Entre Capa 2 y Capa 3** ambas usan `keyBy(betReference)`. Para evitar el shuffle redundante, usar `DataStreamUtils.reinterpretAsKeyedStream(stream, betRefSelector)` — Flink reusa la partición existente y los datos pasan in-process.
> - **El único sink es Pulsar** (`enriched-bets` + 3 DLQs). StarRocks (archivo histórico) y Bet Ticker UI (Splunk, projection en tiempo real) se subscriben al topic `enriched-bets` por su cuenta — fuera del job.
> - **Lazy loading desde StarRocks**: Capa 2 hace JDBC lookup contra StarRocks (`enriched_bets` table) para rehidratar `BetProjection` cuando el state está vacío (bet placed hace mucho tiempo, TTL evicted o restart del job). StarRocks es la fuente confiable para datos históricos.

### 6.1 Visión general

```
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │  CAPA 1 — INGESTION                                                                  │
 │                                                                                      │
 │  Pulsar placement (CloudEvents JSON, 6 topics):                                      │
 │     • bet.captured.v1   • bet.accepted.v1   • bet.placed.v1                          │
 │     • bet.rejected.v1   • bet.failed.v1     • bet.rollback.v1                        │
 │  Pulsar settlement (1 topic, 4 ce-type):                                             │
 │     • settlement-events (leg.graded / bet.graded / bet.settled /                     │
 │                          bet.settlement-failed)                                      │
 │  Pulsar catálogos globales (compactados):                                            │
 │     • catalog.sports, tournaments, market-definitions,                               │
 │       fixtures, markets, competitors                                                 │
 │                                                                                      │
 │  Por topic: PulsarSource → map(canonicalize a POJO BetEvent / CatalogUpdate).        │
 │  Sin watermarks ni dedupe — el ordering y la idempotencia las resuelve el            │
 │  event sourcing de Capa 2.                                                           │
 │                                                                                      │
 │  Salida: stream(BetEvent) + stream(CatalogUpdate)                                    │
 └──────────────────────────────────────────────────────────────────────────────────────┘
                              │ BetEvent (POJO)
                              ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │  CAPA 2 — BET STATUS MANAGER                                                         │
 │                                                                                      │
 │  KeyedProcessFunction (keyBy(betReference)):                                         │
 │     ValueState<BetProjection>    projection      // snapshot acumulado               │
 │     MapState<Long, BetEvent>     eventLog        // log canónico para replay         │
 │     ValueState<Long>             lastAppliedSeq                                      │
 │     TTL: 90 días post-estado terminal (Settled / Rejected / Failed)                  │
 │                                                                                      │
 │  Lógica por evento entrante:                                                         │
 │     1. Si state vacío y evento es de settlement → rehidratar vía JDBC                │
 │        contra StarRocks (SELECT payload FROM enriched_bets WHERE bet_reference=?).   │
 │     2. Validar transición vs state machine (§7.1).                                   │
 │     3. Si (betReference, sequenceNumber) ya está en eventLog → descarta duplicado.   │
 │     4. Insertar en eventLog por sequenceNumber (= (eventTypeOrder << 48) | ceTimeMs).│
 │     5. Si event.seq < lastAppliedSeq → replay completo del log en orden,             │
 │        reconstruyendo projection desde cero.                                         │
 │     6. Aplicar dispatcher por tipo de evento (apply{Captured,Accepted,Placed,…}).    │
 │     7. Emitir BetProjection (con dim refs, sin dim data; enrichment=PENDING).        │
 │                                                                                      │
 │  Side outputs: invalid-transitions                                                   │
 │  No usa broadcast; testeable de forma aislada.                                       │
 └──────────────────────────────────────────────────────────────────────────────────────┘
                              │ BetProjection
                              │ (reinterpretAsKeyedStream → sin shuffle)
                              ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │  CAPA 3 — BET ENRICHER                                                               │
 │                                                                                      │
 │  KeyedBroadcastProcessFunction (key=betReference, broadcast=catalogs):               │
 │                                                                                      │
 │  Broadcast state GLOBAL — todas las dimensiones, con versionado temporal:            │
 │     MapState<String, List<SportVersion>>       sportDesc                             │
 │     MapState<String, List<TournamentVersion>>  tournamentDesc                        │
 │     MapState<String, List<MarketDefVersion>>   marketDefDesc                         │
 │     MapState<String, List<FixtureVersion>>     fixtureDesc                           │
 │     MapState<String, List<MarketVersion>>      marketDesc                            │
 │     MapState<String, List<CompetitorVersion>>  competitorDesc                        │
 │     (opcional: operatorDesc, brandDesc, offeringDesc)                                │
 │                                                                                      │
 │  Cada List se mantiene ordenada ascendente por validFromMs.                          │
 │  Lookup uniforme: VersionLookup.floor(list, eventCeTimeMs) → versión con mayor       │
 │  validFromMs <= eventCeTimeMs, equivalente a TreeMap.floorEntry.                     │
 │  (TreeMap no se usa porque Flink no lo soporta nativamente; ver §6.4.8.)             │
 │                                                                                      │
 │  Keyed state (sólo si enrichment requiere retry):                                    │
 │     ValueState<EnrichmentRequest>  pendingLookups                                    │
 │     ValueState<Integer>            retryCount                                        │
 │                                                                                      │
 │  Lookup ESPECÍFICO (sólo punter — cardinalidad muy alta, no broadcast):              │
 │     ValueState<PunterProfile>      punterCache  (con TTL)                            │
 │     Pulsar source secundario: punter.profile-cdc                                     │
 │                                                                                      │
 │  Lógica por BetProjection entrante:                                                  │
 │     1. Si metadata.enrichment == SUCCESS → passthrough (ya enriquecido).             │
 │     2. Si PENDING → resolver dims con VersionLookup.floor() sobre cada List.         │
 │        Para punter: keyed state local.                                               │
 │     3. Si todos resueltos → enrichment=SUCCESS, emite EnrichedBet.                   │
 │     4. Si alguno falla → EnrichmentRequest + retry exponencial                       │
 │        (backoff 1,2,4,8,16,32,60,60 s; máx 8 intentos).                              │
 │     5. Tras agotar retries → side output enrichment-failed.                          │
 │                                                                                      │
 │  Drenado oportunista en processBroadcastElement (applyToKeyedState)                  │
 └──────────────────────────────────────────────────────────────────────────────────────┘
                              │ EnrichedBet (snapshot completo, §5.2)
                              ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │  CAPA 4 — SINK (sólo Pulsar)                                                         │
 │                                                                                      │
 │     ├─► Pulsar Sink:  enriched-bets (compactado, key=betRef)                         │
 │     ├─► Pulsar Sink:  dlq.enrichment-failed                                          │
 │     └─► Pulsar Sink:  dlq.invalid-transitions                                        │
 │                                                                                      │
 │  Downstream (fuera del job):                                                         │
 │   • StarRocks consume y archiva el topic (sirve queries históricas).                 │
 │   • Bet Ticker UI (Splunk) projecta en tiempo real el último estado por bet,         │
 │     y consulta StarRocks vía JDBC add-on para lecturas históricas.                   │
 │                                                                                      │
 │  Lazy-loading bidireccional:                                                         │
 │   • Capa 2 ⇄ StarRocks: rehidrata BetProjection cuando el state está vacío.          │
 └──────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Job graph completo (10 eventos × 4 capas)

Layout: bandas horizontales por capa, con dos flujos paralelos en Capa 1 (eventos de bet → Capa 2 → Capa 3; catálogos → broadcast → Capa 3 directamente, sin pasar por Capa 2).

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ CAPA 1 — INGESTION                                                                     │
├────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│   Placement events (6 topics):              Catalog topics (compactados):              │
│     bet.captured.v1                           catalog.sports                           │
│     bet.accepted.v1                           catalog.tournaments                      │
│     bet.placed.v1                             catalog.market-definitions               │
│     bet.rejected.v1                           catalog.fixtures                         │
│     bet.failed.v1                             catalog.markets                          │
│     bet.rollback.v1                           catalog.competitors                      │
│                                               (op.: catalog.operators/brands/offerings)│
│   Settlement events (1 topic):                                                         │
│     leg.graded.v1                                                                      │
│     bet.graded.v1                                                                      │
│     bet.settled.v1                                                                     │
│     bet.settlement-failed.v1                                                           │
│                                                                                        │
│        │                                              │                                │
│        ▼ canonicalize() → BetEvent (POJO)             ▼ canonicalize() → CatalogUpdate │
│                                                                                        │
└────────┬───────────────────────────────────────────────┬───────────────────────────────┘
         │                                               │
         │ keyBy(betReference)                           │ broadcast(CatalogUpdate)
         ▼                                               │
┌────────────────────────────────────────────────┐       │
│ CAPA 2 — BET STATUS MANAGER                    │       │
│  KeyedProcessFunction                          │       │
├────────────────────────────────────────────────┤       │
│                                                │       │
│   ValueState<BetProjection> projection         │       │
│   MapState<Long, BetEvent>  eventLog           │       │
│   ValueState<Long>          lastAppliedSeq     │       │
│                                                │       │
│   • Dedup por (betRef, seq) en eventLog        │       │
│   • Validar state machine                      │       │
│   • Insertar en eventLog → replay si late      │       │
│   • Aplicar dispatcher por tipo de evento      │       │
│   • Rehidratación lazy desde StarRocks (JDBC)  │       │
│     ─ ─ ─ ─ ─►  enriched_bets table            │       │
│   side output: invalid-transitions ───┐        │       │
│   (sub-código INVALID_FROM_STATE       │        │       │
│    o MISSING_PREREQUISITE)             │        │       │
│   emit BetProjection (enrichment=PENDING)      │       │
│                                                │       │
└────────┬───────────────────────────────┬───────┘       │
         │ BetProjection                 │               │
         │ reinterpretAsKeyedStream      │               │
         │ (sin shuffle)                 │               │
         ▼                               │               │
┌────────────────────────────────────────┴───────┐       │
│ CAPA 3 — BET ENRICHER                          │ ◄─────┘ broadcast
│  KeyedBroadcastProcessFunction                 │
├────────────────────────────────────────────────┤
│                                                │
│   Broadcast state GLOBAL (todas las dims):     │
│     MapState<String, List<*Version>>           │
│       sport / tournament / market-def /        │
│       fixture / market / competitor            │
│       (+ operator/brand/offering opcional)     │
│     Cada List ordenada por validFromMs ASC.    │
│                                                │
│   Keyed state ESPECÍFICO:                      │
│     ValueState<PunterProfile>  punterCache     │
│     ValueState<EnrichmentRequest> pending      │
│                                                │
│   • SUCCESS → passthrough                      │
│   • PENDING → VersionLookup.floor(list, ts)    │
│   • Retry exponencial + drenado oportunista    │
│                                                │
│   side output: enrichment-failed ──────┐       │
│                                        │       │
│   emit EnrichedBet (enrichment=SUCCESS)│       │
│                                        │       │
└─────────────┬──────────────────────────┴───────┘
              │                          │
              │ EnrichedBet              │ side outputs
              ▼                          ▼
┌──────────────────────────┐  ┌──────────────────────────────────────┐
│ CAPA 4 — SINK            │  │ CAPA 4 — DLQ Sinks                   │
├──────────────────────────┤  ├──────────────────────────────────────┤
│ Pulsar Sink              │  │ Pulsar Sinks (× 2):                  │
│  enriched-bets           │  │   dlq.enrichment-failed              │
│  (compactado,            │  │   dlq.invalid-transitions            │
│   key = bet_reference)   │  │                                      │
│                          │  │                                      │
│ Downstream (fuera del    │  │ (consumidos por equipos de ops o un │
│ job): StarRocks, Splunk, │  │  reprocesador automático)            │
│ analytics consumen este  │  │                                      │
│ topic.                   │  │                                      │
└──────────────────────────┘  └──────────────────────────────────────┘
```

**Re-keys / particiones**:

1. **`keyBy(betReference)`** al entrar a Capa 2 — único shuffle real hacia el state machine.
2. **`reinterpretAsKeyedStream(stream, betRef)`** entre Capa 2 y Capa 3 — Flink reutiliza la partición existente; data flow in-process, sin shuffle físico. Mantiene separación de operadores y testabilidad.
3. **`broadcast(CatalogUpdate)`** desde Capa 1 directamente a Capa 3 (NO pasa por Capa 2) — distribución por replicación a todas las subtareas del enricher.

### 6.3 Detalle por capa

#### Capa 1 — Ingestion

Responsabilidad: deserializar y canonicalizar. Sin watermarks ni dedupe — esas garantías las da el event sourcing de Capa 2.

- Un `PulsarSource` por topic; cada uno consume CloudEvents JSON.
- `MapFunction` por topic traduce el payload Pulsar al POJO `BetEvent` (ver §6.4.2) o `CatalogUpdate` (ver §6.4.3) según corresponda. La canonicalización setea campos comunes (`betReference`, `ceId`, `ceTimeMs`, `eventType`, `eventTypeOrder`, `sequenceNumber`) y deja los campos específicos del evento poblados.
- **No hay `WatermarkStrategy`** — usamos *processing time* a nivel de operador para los timers (TTL, retry, etc.). Eventos late se manejan por replay en Capa 2.
- **No hay dedupe por `ce-id`** — el event sourcing de Capa 2 deduplica por `(betReference, sequenceNumber)` al insertar en `eventLog` (si ya existe, descarta).

> Esta simplificación tiene una consecuencia operativa: si el producer reenvía exactamente el mismo evento (mismo `betReference` y mismo `sequenceNumber`), Capa 2 lo descarta. Si reenvía con un `sequenceNumber` distinto (por ejemplo `ceTime` cambió en el reintento), Capa 2 lo aplica como evento nuevo. Confirmar con producer que `ceTime` se preserva en reentregas (§11).

#### Capa 2 — Bet Status Manager

Operador: `KeyedProcessFunction<String, BetEvent, BetProjection>` keyed por `betReference`.

**State**:

```java
ValueState<BetProjection>          projection;       // snapshot acumulado (sin dim data)
MapState<Long, BetEvent>           eventLog;         // key = sequenceNumber sintético
ValueState<Long>                   lastAppliedSeq;
ValueState<Long>                   ttlExpirationTimer;
```

TTL: 90 días desde la transición a estado terminal (`Settled`/`Rejected`/`Failed`).

**Sequencia sintética** (mientras no haya `sequenceNumber` explícito del producer, §11): `seq = (eventTypeOrder << 48) | (ceTimeMs)` — combinable y monótono dentro del orden lógico.

**Lógica por evento entrante**:

1. **Dedup**: si `eventLog.contains(event.sequenceNumber())` y el contenido es idéntico → descarta como duplicado.

2. **Rehidratación lazy**: si `projection.value() == null` y el evento es de settlement (`LegGraded` / `BetGraded` / `BetSettled` / `BetSettlementFailed`), cargar el último snapshot conocido vía **JDBC lookup contra StarRocks** (tabla `enriched_bets`, PK `bet_reference`):
   ```sql
   SELECT payload FROM betticker.enriched_bets WHERE bet_reference = ?
   ```
   Hidratar `projection` y `lastAppliedSeq` desde el blob JSON. Si no existe (bet nunca enriquecida con anterioridad) → loggear y emitir a `invalid-transitions`.
   Se usa StarRocks (no el topic compactado de Pulsar) porque StarRocks tiene retención histórica completa garantizada; el topic compactado puede haber eviccionado bets viejas. El conector JDBC se configura con HikariCP y timeout duro 2 s.

3. **Validación de transición** vs state machine (§7.1). Si inválida → side output `invalid-transitions` (no aborta el job).

4. **Insertar en `eventLog`** por sequenceNumber; persiste para replay.

5. **Replay si late event**: si `event.seq < lastAppliedSeq` (out-of-order):
   - Reset `projection = null`.
   - Iterar `eventLog` ordenado por seq y re-aplicar cada evento al dispatcher.
   - Emite snapshot final una sola vez (no en cada paso del replay).

5. **Aplicar dispatcher** (caso happy path, en orden):

| Evento | Acción sobre `BetProjection` |
|---|---|
| `Captured` | Crea documento. Setea `betslipId`, `punterId`, `operatorId/brandId/offeringId`, `clientDisplayed*`, `capturedMetadata`, `bet.{reference,type,stake,currency,toWin,potentialPayout}`, `bet.legs[].selections[].id/fixtureId/marketId`, `betStatus="Requested"`, `metadata.enrichment=PENDING` |
| `Accepted` | `bet.acceptedOdds`, `bet.legs[].acceptedOdds`, `bet.legs[].selections[].acceptedOdds`, `betStatus="Accepted"` |
| `Placed` | `ticketId`, `bet.id`, `bet.placedAt`, `bet.legs[].id`, `bet.walletDebit`, `betStatus="Placed"` |
| `Rejected` | `betStatus="Rejected"`, `bet.rejection.{code,message,rejectedAt}` |
| `Failed` | `betStatus="Failed"`, `metadata.failure.{code,message,recoveryAction}` |
| `Rollback` | `betStatus="Failed"`, `metadata.rollback.{id,reason,walletRefunded,refundTransactionId}` |
| `LegGraded` | `bet.legs[legId].{gradeStatus, gradedAt, gradedBy}` |
| `BetGraded` | `bet.gradeStatusId`, `bet.gradeStatusName` |
| `Settled` | `betStatus="Settled"`, `bet.settlement.*`, `settlementId`, `previousSettlementId` |
| `SettlementFailed` | `metadata.settlementRetries++`, metadata.lastSettlementFailure |

7. **Emitir `BetProjection`** (snapshot completo, con `metadata.enrichment` reflejando si requiere enrichment).

**Side output**: `invalid-transitions` con sub-código `INVALID_FROM_STATE` o `MISSING_PREREQUISITE` (ver §6.5).

#### Capa 3 — Bet Enricher

Operador: `KeyedBroadcastProcessFunction<String, BetProjection, CatalogUpdate, EnrichedBet>` keyed por `betReference`.

**Broadcast state — TODAS las dimensiones globales con versionado temporal (ver §4.2)**:

```java
// Cada catálogo guarda historial temporal por entidad como List ordenada por validFromMs ASC.
// NOTA: NO se usa TreeMap porque Flink no tiene serializer nativo para TreeMap/SortedMap
//       (caería a Kryo, rompiendo schema evolution — ver §6.4.8).
MapStateDescriptor<String, List<SportVersion>>       sportDesc;
MapStateDescriptor<String, List<TournamentVersion>>  tournamentDesc;
MapStateDescriptor<String, List<MarketDefVersion>>   marketDefDesc;
MapStateDescriptor<String, List<FixtureVersion>>     fixtureDesc;
MapStateDescriptor<String, List<MarketVersion>>      marketDesc;
MapStateDescriptor<String, List<CompetitorVersion>>  competitorDesc;
// opcional (pendiente confirmar, §11):
MapStateDescriptor<Integer, OperatorVersion>   operatorDesc;
MapStateDescriptor<Integer, BrandVersion>      brandDesc;
MapStateDescriptor<Integer, OfferingVersion>   offeringDesc;
```

**Invariante de inserción** — al recibir un `CatalogUpdate` en `processBroadcastElement`, la versión nueva se inserta en posición ordenada por `validFromMs` (binary insertion):

```java
public void processBroadcastElement(CatalogUpdate u, Context ctx, Collector<EnrichedBet> out) {
    if ("Fixture".equals(u.getCatalogType())) {
        FixtureVersion v = u.getFixture();
        BroadcastState<String, List<FixtureVersion>> state = ctx.getBroadcastState(fixtureDesc);
        List<FixtureVersion> versions = state.get(v.getId());
        if (versions == null) versions = new ArrayList<>();
        int pos = lowerBoundByValidFromMs(versions, v.getValidFromMs());
        versions.add(pos, v);
        // opcional: cap a las N versiones más recientes para limitar tamaño
        state.put(v.getId(), versions);
    }
    // ... similar para sport / tournament / marketDef / market / competitor
}
```

**Patrón de lookup uniforme** (equivalente a `TreeMap.floorEntry`):

```java
List<FixtureVersion> versions = ctx.getBroadcastState(fixtureDesc).get(fixtureId);
FixtureVersion vigente = VersionLookup.floor(versions, eventCeTimeMs, FixtureVersion::getValidFromMs);
```

`VersionLookup.floor` (definido en §6.4.9) hace búsqueda binaria sobre la `List` ordenada y devuelve la versión con mayor `validFromMs <= eventCeTimeMs` — la oferta que el punter vio cuando hizo la apuesta, no la actual.

**Keyed state (sólo si enrichment requiere retry o para Punter)**:

```java
ValueState<EnrichmentRequest> pendingLookups;
ValueState<Integer>           retryCount;
ValueState<PunterProfile>     punterCache;     // TTL configurable
```

**Punter como única dimensión ESPECÍFICA** (cardinalidad muy alta, no apta para broadcast a todas las subtareas): se mantiene como keyed state local, hidratado desde un Pulsar source secundario (`punter.profile-cdc`) o vía lookup contra el topic compactado al primer evento de la bet.

**Lazy loading desde StarRocks** (similar a Capa 2, pero por dimensión faltante):

El broadcast state y el `punterCache` pueden tener entries faltantes en dos situaciones:
- **TTL eviction**: una versión vieja de una dim global (p.ej. `tournamentVersion` con `validFrom = hace 6 meses`) fue eviccionada del broadcast state por TTL.
- **Punter cache miss**: la primera vez que el enricher ve a un punter o el cache se expiró.

En ambos casos hay que recurrir a StarRocks:

```sql
-- ejemplo: tournament version vigente al timestamp de la bet
SELECT * FROM offer.tournaments_history
WHERE tournament_id = ?
  AND valid_from <= ?
ORDER BY valid_from DESC LIMIT 1;

-- ejemplo: punter profile
SELECT * FROM punter.profile_dim WHERE punter_id = ?;
```

El resultado se inyecta en el broadcast state (o `punterCache`) y se procede con el enrichment normal. Si el lookup falla (la versión nunca existió o el punter no existe), va al retry buffer y eventualmente al side output `enrichment-failed`.

**Lógica por `BetProjection` entrante**:

1. Si `projection.metadata.enrichment == SUCCESS`: **passthrough** — la bet ya fue enriquecida en un evento anterior; los dim data ya están en el snapshot. Emite directo como `EnrichedBet`.

2. Si `enrichment == PENDING` (o `PARTIAL` desde retry previo):
   - Para cada `selection`:
     - `fixture` por `fixtureId` → `VersionLookup.floor(fixtureDesc.get(fixtureId), eventCeTime, FixtureVersion::getValidFromMs)`
     - `market` por `marketId` → `VersionLookup.floor(marketDesc.get(marketId), eventCeTime, MarketVersion::getValidFromMs)`
     - `sport` por `fixture.sportId` → `VersionLookup.floor(sportDesc.get(sportId), eventCeTime, ...)`
     - `tournament` por `fixture.tournamentId` → `VersionLookup.floor(tournamentDesc.get(tournamentId), eventCeTime, ...)`
     - `competitors[]` por `fixture.competitorIds[]` → idem por cada `competitorId`
     - `market.defId` resuelve template adicional desde `marketDefDesc`
   - `punter.limits` por `punterId` → keyed state `punterCache` (o lookup CDC).

3. Si todas las dimensiones resolvieron:
   - Rellena `selection.{sportId, sportName, tournamentId, tournamentName, fixture, market}`, `punter.limits`.
   - Marca `metadata.enrichment = SUCCESS`.
   - Emite `EnrichedBet`.

4. Si alguna falla (catálogo aún no recibido para ese id/timestamp):
   - Guarda `EnrichmentRequest` con los ids pendientes en `pendingLookups`.
   - Programa retry timer con backoff exponencial: `1, 2, 4, 8, 16, 32, 60, 60 s` (máx 8 intentos / 5 min).
   - Si todavía falla tras 8 intentos → side output `enrichment-failed`.

**Drenado oportunista**: `processBroadcastElement` usa `applyToKeyedState` para reintentar drenar las pending bets cuando llega una versión nueva del catálogo, sin esperar al próximo timer.

#### Capa 4 — Sink (sólo Pulsar)

- **Pulsar Sink — `enriched-bets`** (`PulsarSink<EnrichedBet>`):
  - Topic: `persistent://sportsbook/betting/enriched-bets`
  - Key del mensaje = `bet_reference` → garantiza orden por bet en el consumer.
  - **Compactación habilitada** → consumers nuevos reciben el último estado por bet.
  - Esquema Avro (ver §6.4 para detalles sobre POJO + schema evolution).
- **DLQ Sinks** (Pulsar topics dedicados, side outputs del job — ver §6.5):
  - `dlq.enrichment-failed`: bets que no pudieron enriquecerse tras 8 retries. Payload incluye razón.
  - `dlq.invalid-transitions`: eventos rechazados por la state machine, con sub-código `INVALID_FROM_STATE` o `MISSING_PREREQUISITE`.

> **Por qué sólo Pulsar**: StarRocks (serving del ticker), Splunk (logs/compliance) y otros consumidores se subscriben al topic `enriched-bets` y persisten/procesan por su cuenta — fuera del job de Flink. Esto desacopla la pipeline del modelo físico de cada destino y simplifica el job a un único tipo de sink (con su mismo conector Pulsar-Flink).

---

### 6.4 Modelos de datos compartidos entre capas

> **Restricción de plataforma**: Flink 1.18 + Pulsar-Flink connector. Por compatibilidad con el connector y con savepoints / schema evolution, los modelos NO usan `record` (Java 16+). Se diseñan como **POJOs Flink-serializables**:
> - Clase `public`, no genérica.
> - Constructor `public` sin argumentos.
> - Todos los campos `private` con getters/setters `public` (convención JavaBean), o `public` directamente.
> - Tipos de campo deben ser ellos mismos Flink-serializables (POJO, primitivo, `String`, `Instant` desde Flink 1.18, `BigDecimal`, `List`/`Map` con tipos genéricos POJO).
> - **No usar Kryo**: se desactiva con `env.getConfig().disableGenericTypes()` en producción para fallar fast si se usa un tipo no POJO. Forzamos el `PojoTypeInfo` (o Avro) en todos los `TypeInformation`.

#### 6.4.1 Schema evolution con Flink savepoints

Flink 1.18 soporta dos serializadores estables ante savepoint upgrade:
1. **`PojoSerializer`**: tolera *añadir campos al final* y *eliminar campos en desuso*. **No** tolera renombrar, cambiar tipo, ni reordenar.
2. **`AvroSerializer`** (recomendado para schemas que evolucionarán mucho): tolera reglas Avro de compatibilidad (add field con default, deprecate, etc.). Requiere registrar `AvroTypeInfo` via `@TypeInfo` annotation o explícitamente.

**Para este job recomendamos Avro** en todos los modelos que viven en `MapState`/`ValueState`/`BroadcastState` (Capa 2 y Capa 3). Genera POJOs con avro-maven-plugin a partir de `.avsc`/`.avdl`, registra `AvroTypeInfo` en cada `MapStateDescriptor`.

**Patrón crítico — variantes / tipos union**: con `PojoSerializer` los sealed interfaces NO son serializables y la herencia es frágil. Solución: usar **un único POJO con campo discriminador `type` (string) + campos opcionales por variante** (sólo el correspondiente al `type` tiene valor). Añadir una nueva variante = añadir un nuevo campo opcional al final (no rompe savepoints porque PojoSerializer/Avro toleran add-field-with-default).

> **Respuesta a la pregunta del usuario**: si mañana añades un `MarketLineEntry` a `CatalogUpdate`, el patrón discriminador permite hacerlo sin invalidar el savepoint: agregas el campo `marketLineEntry` (opcional) al POJO `CatalogUpdate`, deployás con el savepoint anterior, los registros viejos (con `marketLineEntry == null`) siguen funcionando, los nuevos lo pueblan. Si usaras sealed interface o herencia, romperías el savepoint y tendrías que reiniciar desde cero o migrar manualmente.

#### 6.4.2 `BetEvent` (Capa 1 → Capa 2)

POJO único con `eventType` como discriminador. Sólo los campos del tipo correspondiente se pueblan (resto en `null`). Añadir un nuevo tipo de evento = añadir un nuevo campo `xPayload` al final.

```java
public class BetEvent {
    // identidad común (siempre poblada)
    private String  betReference;
    private String  ceId;
    private long    ceTimeMs;             // long, no Instant: compatible Avro/POJO
    private String  eventType;            // "Captured" | "Accepted" | "Placed" | ...
    private int     eventTypeOrder;       // 0..9
    private long    sequenceNumber;       // (eventTypeOrder << 48) | ceTimeMs

    private String  betslipId;
    private String  punterId;
    private Integer operatorId;
    private Integer brandId;
    private Integer offeringId;

    // payloads por tipo de evento — sólo uno está poblado, resto null
    private CapturedPayload         capturedPayload;
    private AcceptedPayload         acceptedPayload;
    private PlacedPayload           placedPayload;
    private RejectedPayload         rejectedPayload;
    private FailedPayload           failedPayload;
    private RollbackPayload         rollbackPayload;
    private LegGradedPayload        legGradedPayload;
    private BetGradedPayload        betGradedPayload;
    private SettledPayload          settledPayload;
    private SettlementFailedPayload settlementFailedPayload;

    public BetEvent() {} // no-arg constructor REQUERIDO por POJO serializer

    // getters / setters para cada campo (omitidos por brevedad)
}

public class CapturedPayload {
    private String acceptOddsChangesMode;
    private String clientOddsFormat;
    private String clientCurrency;
    private String clientDisplayedStake;
    private String clientDisplayedToWin;
    private String clientDisplayedPayout;
    private CapturedBetData bet;
    private CapturedMetadata metadata;
    public CapturedPayload() {}
    // getters/setters
}

public class AcceptedPayload {
    private AcceptedBetData bet;
    private List<ValidationStep> validationTrail;
    private int validationDurationMs;
    public AcceptedPayload() {}
    // getters/setters
}

public class PlacedPayload {
    private String ticketId;
    private PlacedBetData bet;
    private WalletDebit wallet;
    private LiveDelay liveDelay;
    public PlacedPayload() {}
    // getters/setters
}

// ...RejectedPayload, FailedPayload, RollbackPayload, LegGradedPayload,
//    BetGradedPayload, SettledPayload, SettlementFailedPayload
//   (mismo patrón: clase, no-arg constructor, getters/setters)
```

POJOs auxiliares (todos siguen las mismas reglas: clase, no-arg constructor, getters/setters):

```java
public class CapturedBetData {
    private String betReference;
    private String type;                  // BetType: "Straight" | "Parlay"
    private BigDecimal stake;
    private String currency;
    private ComputedOdds punterOdds;
    private BigDecimal toWin;
    private BigDecimal potentialPayout;
    private List<CapturedLeg> legs;
    public CapturedBetData() {}
    // getters/setters
}

public class CapturedLeg {
    private String type;                  // LegType
    private ComputedOdds punterOdds;
    private List<CapturedSelection> selections;
    public CapturedLeg() {}
    // getters/setters
}

public class CapturedSelection {
    private String id;
    private VersionedOdds punterOdds;
    private ClientSportSnapshot sport;
    private List<ClientCategorySnapshot> categories;
    private ClientTournamentSnapshot tournament;
    private ClientFixtureSnapshot fixture;
    private ClientMarketSnapshot market;
    public CapturedSelection() {}
    // getters/setters
}

public class ComputedOdds {
    private BigDecimal decimal;
    private int american;
    private Fractional fractional;
    public ComputedOdds() {}
    // getters/setters
}

public class VersionedOdds {
    private String version;
    private BigDecimal decimal;
    private int american;
    private Fractional fractional;
    private Long capturedAtMs;            // long, nullable
    public VersionedOdds() {}
    // getters/setters
}

public class Fractional {
    private int numerator;
    private int denominator;
    public Fractional() {}
    // getters/setters
}

// Enums como String en los POJOs serializados (Avro-friendly); las constantes existen como enum Java para uso en código:
public enum BetType   { Straight, Parlay }
public enum LegType   { Simple, SGP, Composite }
public enum GradingResult     { Pending, Cancelled, Won, Lost, Void, Push }
public enum SettlementResult  { Pending, Won, Lost, Void }
public enum WalletOperationType { SettlementWon, SettlementLost, SettlementVoid, SettlementNoAction }
public enum BetStatus { Requested, Accepted, Placed, Rejected, Failed, Settled }
```

> **Por qué los enums se serializan como `String` y no como Java enum**: Flink `PojoSerializer` soporta enums pero **no tolera añadir nuevos valores** en savepoint upgrade. Si mañana añades `Push2` al `GradingResult` y restartas desde un savepoint con sólo 6 valores, el serializer puede fallar. Usar `String` evita el problema y mantiene compatibilidad Avro. Validamos contra el enum en código.

#### 6.4.3 `CatalogUpdate` (Capa 1 → Capa 3 vía broadcast)

POJO discriminado. Añadir un nuevo tipo de catálogo (p.ej. `marketLineEntry`) = añadir nuevo campo opcional al final. **Schema-evolvable sin invalidar savepoint**.

```java
public class CatalogUpdate {
    private String catalogType;               // "Sport" | "Tournament" | "MarketDef" | "Fixture" |
                                              // "Market" | "Competitor" | "Operator" | "Brand" | "Offering"
    private long   validFromMs;
    // sólo uno poblado, según catalogType
    private SportVersion       sport;
    private TournamentVersion  tournament;
    private MarketDefVersion   marketDef;
    private FixtureVersion     fixture;
    private MarketVersion      market;
    private CompetitorVersion  competitor;
    private OperatorVersion    operator;
    private BrandVersion       brand;
    private OfferingVersion    offering;
    public CatalogUpdate() {}
    // getters/setters
}

public class SportVersion {
    private String id, name, sportType, ruleSet;
    private long validFromMs;
    public SportVersion() {}
    // getters/setters
}

public class TournamentVersion {
    private String id, name, sportId;
    private List<String> categoryIds;
    private long validFromMs;
    public TournamentVersion() {}
    // getters/setters
}

public class MarketDefVersion {
    private String id, name, type, timeFrame;
    private long validFromMs;
    public MarketDefVersion() {}
    // getters/setters
}

public class FixtureVersion {
    private String id, type, name;
    private long   dateMs;
    private String phase, status, timeFrame, clientName;
    private Long   clientDateMs;
    private List<String> competitorIds;
    private long   validFromMs;
    public FixtureVersion() {}
    // getters/setters
}

public class MarketVersion {
    private String id, defId, type, status, name, clientName;
    private String selectionStatus, selectionName, clientSelectionName;
    private VersionedOdds odds, clientOdds;
    private long   validFromMs;
    public MarketVersion() {}
    // getters/setters
}

public class CompetitorVersion {
    private String id, name, clientName;
    private long validFromMs;
    public CompetitorVersion() {}
    // getters/setters
}

public class OperatorVersion { private int id; private String name; private long validFromMs; /* ... */ }
public class BrandVersion    { private int id; private String name; private int operatorId; private long validFromMs; /* ... */ }
public class OfferingVersion { private int id; private String name; private int brandId; private long validFromMs; /* ... */ }
```

#### 6.4.4 `BetProjection` (Capa 2 → Capa 3)

POJO. Mismo patrón de discriminador implícito: los campos de enrichment están `null` hasta que Capa 3 los rellena.

```java
public class BetProjection {
    // identidad
    private String  betReference;
    private String  ticketId;             // null hasta Placed
    private String  betslipId;
    private Integer operatorId, brandId, offeringId;
    private String  punterId;

    // client display (de Captured ECST)
    private String acceptOddsChangesMode;
    private String clientLanguage, clientLocale, clientOddsFormat, clientCurrency;
    private String clientDisplayedStake, clientDisplayedToWin, clientDisplayedPayout;

    private CapturedMetadata capturedMetadata;
    private PunterRef        punter;       // {id; limits=null hasta Capa 3}
    private Bet              bet;
    private ProcessingMetadata metadata;

    public BetProjection() {}
    // getters/setters
}

public class ProcessingMetadata {
    private String srcEventId;
    private String srcEventType;
    private long   srcTimeMs;
    private String enrichment;             // "PENDING" | "PARTIAL" | "SUCCESS" | "FAILED"
    private int    retry;
    private int    version;
    private long   lastAppliedSeq;
    private int    lastEventTypeOrder;
    private Map<String, String> enrichmentFailures;
    private ErrorDetails        failure;
    private RollbackDetails     rollback;
    private int    settlementRetries;
    public ProcessingMetadata() {}
    // getters/setters
}

public class PunterRef {
    private String id;
    private PunterLimits limits;           // null hasta Capa 3
    public PunterRef() {}
    // getters/setters
}

public class PunterLimits { /* a definir, §11 punto 9 */ public PunterLimits() {} }

public class Bet {
    private String reference, id;
    private String type;                   // BetType as String
    private Long   placedAtMs;
    private BigDecimal stake;
    private String     currency;
    private ComputedOdds acceptedOdds;
    private BigDecimal toWin, potentialPayout;
    private List<Leg>  legs;
    private String     betStatus;          // BetStatus as String
    private Integer    gradeStatusId;
    private String     gradeStatusName;
    private WalletDebit       walletDebit;   // null hasta Placed
    private Settlement        settlement;    // null hasta Settled
    private RejectionDetails  rejection;     // null salvo Rejected
    public Bet() {}
    // getters/setters
}

public class Leg {
    private String id, type;               // legId, LegType
    private int    position;
    private ComputedOdds acceptedOdds;
    private String       gradeStatus;      // GradingResult as String
    private Long         gradedAtMs;
    private String       gradedBy;
    private List<Selection> selections;
    public Leg() {}
    // getters/setters
}

public class Selection {
    // ids del evento (siempre presentes desde Accepted)
    private String  id;                    // selectionId
    private String  fixtureId, marketId;
    private Boolean isLive;
    private VersionedOdds acceptedOdds;

    // enrichment GLOBAL (null hasta Capa 3)
    private String sportId, sportName;
    private String tournamentId, tournamentName;

    // enrichment GLOBAL (null hasta Capa 3)
    private Fixture fixture;
    private Market  market;

    public Selection() {}
    // getters/setters
}

public class Fixture {
    private String id, type, name;
    private long   dateMs;
    private String phase, status, timeFrame, clientName;
    private Long   clientDateMs;
    private List<Competitor> competitors;
    public Fixture() {}
    // getters/setters
}

public class Competitor {
    private String id, name, clientName, position;
    public Competitor() {}
    // getters/setters
}

public class Market {
    private String id, defId, type, status, name, clientName;
    private String selectionStatus, selectionName, clientSelectionName;
    private VersionedOdds odds, clientOdds;
    public Market() {}
    // getters/setters
}

public class Settlement {
    private Integer result;
    private String  settledBy, walletTransactionId, walletOperation;
    private BigDecimal walletAmountValue;
    private String  walletCurrency, settlementId, previousSettlementId;
    public Settlement() {}
    // getters/setters
}
```

#### 6.4.5 `EnrichedBet` (Capa 3 → Capa 4)

**Es el mismo tipo `BetProjection` con `metadata.enrichment == "SUCCESS"`** y los campos `selection.{sportId, sportName, tournamentId, tournamentName, fixture, market}` y `punter.limits` poblados. Usar la misma clase elimina conversión y reduce overhead de serialización.

```java
public final class EnrichedBet {
    public static BetProjection ensureEnriched(BetProjection p) {
        if (!"SUCCESS".equals(p.getMetadata().getEnrichment())) {
            throw new IllegalStateException("Bet not enriched: " + p.getBetReference());
        }
        return p;
    }
}
```

> **Por qué no dos clases distintas**: forzaría un mapping completo en el enricher (copia campo por campo) y rompería la igualdad estructural. Manteniendo un solo `BetProjection` con bandera de estado, el enricher sólo rellena campos.

#### 6.4.6 Modelos auxiliares internos a Capa 3

```java
public class EnrichmentRequest {
    private String  betReference;
    private Set<String> pendingFixtureIds;
    private Set<String> pendingMarketIds;
    private Set<String> pendingCompetitorIds;
    private Set<String> pendingSportIds;
    private Set<String> pendingTournamentIds;
    private Set<String> pendingPunterIds;
    private long firstAttemptAtMs, lastAttemptAtMs;
    public EnrichmentRequest() {}
    // getters/setters
}
```

#### 6.4.7 Resumen de los modelos por capa

| Capa | Lee | Escribe / emite | Persiste (state) |
|---|---|---|---|
| 1. Ingestion | CloudEvent JSON desde Pulsar | `BetEvent`, `CatalogUpdate` (POJOs) | — |
| 2. Status Manager | `BetEvent` | `BetProjection` (con `enrichment=PENDING`) | `ValueState<BetProjection>`, `MapState<Long, BetEvent>` (eventLog) |
| 3. Bet Enricher | `BetProjection`, `CatalogUpdate` (broadcast) | `BetProjection` con `enrichment=SUCCESS` (alias `EnrichedBet`) | Broadcast `MapState<String, List<*Version>>` (catálogos, ordenada por `validFromMs`), `ValueState<EnrichmentRequest>`, `ValueState<PunterProfile>` |
| 4. Sink | `BetProjection` (enriched) | Pulsar topics (4) | — |

#### 6.4.8 Reglas operativas para mantener schema-evolvable

1. **No usar `record`** (Java 16+) — incompatibles con `PojoSerializer` y con savepoints estables.
2. **No usar sealed interfaces / herencia** en tipos serializados — usar discriminador `type` + campos opcionales.
3. **No usar Java enums en POJOs serializados** — usar `String` y validar contra enum en código. Excepción: enums totalmente cerrados (p.ej. `Boolean`-like) que jamás añadirán valores.
4. **Timestamps como `long` (epoch ms)** — `Instant` es soportado en Flink 1.18 pero `long` es más portable a Avro/Pulsar schemas.
5. **Añadir campos siempre al final** y siempre opcionales (nullable).
6. **No renombrar campos, no cambiar tipos** — eso rompe el savepoint. Si necesitas cambiar semántica: añade un campo nuevo, deprecate el viejo, migra en background.
7. **Registrar `TypeInformation` explícito** en todos los `MapStateDescriptor` (no confiar en reflection): `new MapStateDescriptor<>("name", TypeInformation.of(K.class), TypeInformation.of(V.class))`.
8. **Deshabilitar Kryo en producción**: `env.getConfig().disableGenericTypes()` para fallar fast si algún tipo no es POJO/Avro.
9. **Colecciones permitidas: solo `List`, `Map`, `Set`, `Collection`** (interfaces, con type arguments concretos). Flink no tiene serializer nativo para subclases concretas como `TreeMap`, `SortedMap`, `LinkedList`, `LinkedHashMap` — caerían a Kryo. Para representar "mapa ordenado" (p.ej. historial de versiones por `validFromMs`), usar `List<V>` mantenida ordenada por inserción + búsqueda binaria. Ver helper `VersionLookup` en §6.4.9.

#### 6.4.9 Helper `VersionLookup` (sustituto de `TreeMap.floorEntry`)

Implementa la búsqueda binaria sobre una `List<V>` ordenada ascendente por `validFromMs` para devolver la versión con mayor `validFromMs <= eventTs`:

```java
import java.util.List;
import java.util.function.ToLongFunction;

public final class VersionLookup {
    private VersionLookup() {}

    /**
     * Devuelve la versión con mayor validFromMs <= eventTs, o null si no existe.
     * Precondición: 'versions' está ordenada ascendente por la clave devuelta por
     * validFromExtractor.
     */
    public static <V> V floor(List<V> versions, long eventTs, ToLongFunction<V> validFromExtractor) {
        if (versions == null || versions.isEmpty()) return null;
        int lo = 0, hi = versions.size() - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long vfm = validFromExtractor.applyAsLong(versions.get(mid));
            if (vfm <= eventTs) { result = mid; lo = mid + 1; }
            else                { hi = mid - 1; }
        }
        return result < 0 ? null : versions.get(result);
    }

    /**
     * Punto de inserción ordenada para una nueva versión con validFromMs = newValidFromMs.
     * Devuelve el índice donde insertar para mantener orden ascendente.
     */
    public static <V> int lowerBound(List<V> versions, long newValidFromMs, ToLongFunction<V> validFromExtractor) {
        int lo = 0, hi = versions.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (validFromExtractor.applyAsLong(versions.get(mid)) < newValidFromMs) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }
}
```

Notas:
- `O(log n)` para lookup, `O(n)` para insert (por el `ArrayList.add(index, e)`). Aceptable porque cada entidad típicamente tiene 1–20 versiones.
- Si una entidad excede un umbral (p.ej. 50 versiones por fixture), implementar política de retención eliminando las más antiguas — el lookup temporal sigue funcionando para placements recientes.

### 6.5 Side outputs y DLQ — casos de emisión

El job emite a **2 topics Pulsar dedicados** de dead-letter cuando detecta condiciones que no puede resolver. Cada uno tiene un caso de uso específico y un equipo responsable distinto:

| DLQ topic | Emite quién | ¿En qué casos? | Acción operativa recomendada |
|---|---|---|---|
| **`dlq.invalid-transitions`** | Capa 2 (BetStatusManager) | El evento no puede aplicarse a la `BetProjection` en su estado actual. Cubre dos sub-categorías (indicadas en `metadata.failureCode` del payload):&lt;br&gt;&lt;br&gt;**`INVALID_FROM_STATE`** — la transición está prohibida por la state machine (§7.1):&lt;br&gt;• `BetAccepted` o `BetPlaced` recibidos sin un `BetCaptured` previo (state = null).&lt;br&gt;• Cualquier evento tras estado terminal (`Rejected` / `Failed`) que NO sea un re-settle válido. P.ej. `LegGraded` sobre una bet `Rejected`.&lt;br&gt;• Duplicado de `BetCaptured` para el mismo `betReference` con payload distinto.&lt;br&gt;&lt;br&gt;**`MISSING_PREREQUISITE`** — el evento llegó pero su evento previo nunca arribó tras un timer de espera (p.ej. 30 min):&lt;br&gt;• `BetSettled` bufferado en `eventLog` esperando `BetPlaced`, que jamás llega.&lt;br&gt;• `LegGraded` con `legId` que no existe en la projection (porque el `BetPlaced` que lo creó nunca llegó). | Investigar el producer upstream del dominio:&lt;br&gt;• `INVALID_FROM_STATE` → producer envía eventos desordenados, duplicados, o para una bet inexistente. Corregir y considerar reproceso manual.&lt;br&gt;• `MISSING_PREREQUISITE` → producer probablemente dropea un evento intermedio antes de publicarlo a Pulsar (no es pérdida en transporte; es bug en producer). Revisar logs del producer del dominio (Bet Placement / Bet Settlement). |
| **`dlq.enrichment-failed`** | Capa 3 (BetEnricher) | El enricher no pudo resolver todas las dimensiones requeridas tras agotar los 8 reintentos con backoff exponencial (~5 min total). Ejemplos:&lt;br&gt;• `fixtureId` referenciado por la bet no está en el broadcast state ni en `offer.fixtures_history` de StarRocks. Probablemente fixture desconocido o ID corrupto.&lt;br&gt;• `marketId` no encontrado tras lazy load — market eliminado del catálogo o ID inválido.&lt;br&gt;• `competitorId` no encontrado.&lt;br&gt;• `punterId` no resoluble: ni en `punterCache`, ni en `punter.profile-cdc`, ni en `punter.profile_dim` de StarRocks.&lt;br&gt;• Versión específica de dim (con `validFrom` específico) no existe — p.ej. el evento referencia una versión de market que jamás se publicó. | Reprocesable: el payload contiene la `BetProjection` completa (no enriquecida) + lista de dimIds faltantes y el último error de lookup. Operativa:&lt;br&gt;• Validar que el catálogo en StarRocks contiene la dim referenciada.&lt;br&gt;• Si la dim aparece tarde (CDC retrasado), reinyectar la bet al topic de placement events para reprocesar.&lt;br&gt;• Si la dim nunca existirá (datos corruptos), aceptar la pérdida y archivar para auditoría. |

**Resumen operacional**:
- `invalid-transitions` → problema en el **producer** del dominio (orden / duplicación / drops de eventos del placement o settlement service).
- `enrichment-failed` → problema en el **catálogo** de oferta o punter (dimensiones faltantes / CDC desincronizado).

> **Sobre detección de pérdida en el transporte Pulsar (no es un DLQ del job)**: con la información actual de los eventos no podemos detectar pérdidas en Pulsar de forma directa, porque los eventos no traen un `sequenceNumber` monotónico emitido por el producer (§11 punto 2). Si en el futuro el producer lo emite, se podría reactivar un tercer DLQ `dlq.sequence-gaps` para detectar gaps reales (seq 1, 2, **5**, 6 → faltan 3 y 4). Mientras tanto, este escenario es indistinguible de un drop del producer y cae en `invalid-transitions / MISSING_PREREQUISITE`.

---

## 7. State machine

### 7.1 Diagrama de estados

Happy path lineal (de izquierda a derecha) con dos terminales de error abajo:

```
                                          LegGraded / BetGraded /
                                          BetSettlementFailed
                                          (self-loop, NO cambian betStatus)
                                              ╭────────╮
                                              ▼        │
   ●────────►┌───────────┐──────────►┌──────────┐─────►┌──────────┐──────────►┌──────────┐──►◉
   start     │ Requested │ Accepted  │ Accepted │Placed│  Placed  │ BetSettled│ Settled  │ final
   Captured  └─────┬─────┘           └────┬─────┘      └──────────┘ post-Graded└────┬─────┘
                   │                      │                                          ▲
                   │ BetRejected          │ BetRejected /                            │ BetSettled
                   │                      │ BetFailed /                              │ (re-settle con
                   ▼                      ▼ BetRollback                              │  previous-
              ┌──────────┐           ┌──────────┐                                    │  SettlementId)
              │ Rejected │           │  Failed  │                                    │
              │ terminal │           │ terminal │                                    └──────────┐
              └──────────┘           └──────────┘                                               │
                                                                                                ╰─ loop on Settled
```

**Tabla de transiciones**:

| Desde \ Hacia | Requested | Accepted | Placed | Settled | Rejected | Failed |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| (null)        | BetCaptured | – | – | – | – | – |
| Requested     | – | BetAccepted | – | – | BetRejected | – |
| Accepted      | – | – | BetPlaced | – | BetRejected | BetFailed / BetRollback |
| Placed        | – | – | self-loop | BetSettled | – | – |
| Settled       | – | – | – | self-loop (re-settle) | – | – |
| Rejected      | – | – | – | – | – | – |
| Failed        | – | – | – | – | – | – |

**Eventos que NO cambian `betStatus`** (aplican sobre `Placed` o `Settled`):

| Evento | Campos modificados | Razón |
|---|---|---|
| `LegGraded` | `bet.legs[legId].{gradeStatus, gradedAt, gradedBy}` | Una leg se grada; bet sigue Placed hasta `BetSettled` |
| `BetGraded` | `bet.gradeStatusId`, `bet.gradeStatusName` | El outcome global ya está, pero la liquidación de wallet falta |
| `BetSettlementFailed` | `metadata.settlementRetries++`, `metadata.lastSettlementFailure` | Métrica interna; bet sigue Placed |
| `BetSettled` (re-settle) | Sobreescribe `bet.settlement` con `previousSettlementId` poblado | Reapplica liquidación tras rollback de settlement |

**Notas**:

- `BetRollback` tras `Accepted` o tras `Placed` (raro): el resultado final es `Failed` con `bet.rollback` poblado.
- Estados terminales (`Settled` / `Rejected` / `Failed`): eventos posteriores van a side output `invalid-transitions`. Excepción: re-settle (`BetSettled` con `previousSettlementId`) sobre `Settled`.

> La matriz de qué se enriquece y desde qué fuente está en §4 (Qué necesita enrichment — Global vs Específico).

---

## 8. Consideraciones operativas

> El dimensionamiento (paralelismo, memoria, throughput esperado) queda pendiente hasta tener volúmenes de datos confirmados (§11 punto 15). Esta sección describe configuraciones cualitativas que no dependen del volumen.

### 8.1 Checkpoints

- Frecuencia: 60 s
- Min pause: 30 s
- Aligned checkpoints; RocksDB local con incremental.
- Recovery esperado: <2 min.

### 8.2 Métricas

| Métrica | Tipo | Acción si dispara |
|---|---|---|
| `statusmgr.invalid_transitions.{INVALID_FROM_STATE,MISSING_PREREQUISITE}` | Counter | Revisar producer del dominio: por sub-código identifica si es transición prohibida o evento sin prerequisito |
| `statusmgr.rehydrations` | Counter | Spike → state TTL muy corto vs. ventana settlement |
| `statusmgr.replays` | Counter | Spike → producer enviando late events |
| `enricher.passthrough_rate` | Gauge | Confirma que la mayoría de eventos saltan enrichment (>80% esperado) |
| `enricher.failures.{fixture,market,sport,tournament,competitor,punter}` | Counter | Por tipo de dim — identifica qué catálogo está atrasado |
| `enricher.retries` | Histogram | Distribución del nº de intentos antes de éxito o DLQ |
| `enricher.broadcast_state_size.{catalog}` | Gauge | Crecimiento desbordado → revisar política de retención |
| `sink.pulsar_publish_errors` | Counter | > 0 → problema con cluster Pulsar |
| `dlq.*` rates | Counter | Cualquier valor > 0 requiere investigación |

### 8.3 Runbook (extracto)

| Síntoma | Causa probable | Acción |
|---|---|---|
| `dlq.enrichment-failed` crece | Catálogo global atrasado (sport/tournament/market-def/fixture/market/competitor) | Verificar lag de los topics catalog.* |
| `dlq.invalid-transitions` crece (INVALID_FROM_STATE) | Producer reordena / duplica eventos | Revisar logs del producer; verificar orden de emisión |
| `dlq.invalid-transitions` crece (MISSING_PREREQUISITE) | Producer dropea un evento intermedio (p.ej. envía Settled sin Placed) | Revisar logs del producer del dominio (Bet Placement / Settlement) |
| Statusmgr.replays elevado | Eventos late frecuentes | Investigar latencia del producer; ajustar TTL del eventLog |
| Spike de rehidrataciones | TTL del state del projector muy corto vs. ventana settlement | Ajustar TTL (default 90 días) o revisar checkpointing |
| Pulsar publish errors | Cluster Pulsar saturado / red | Revisar cluster; verificar back-pressure en el job |

---

## 9. Garantías y propiedades

1. **Idempotencia**: dedupe por `(betReference, sequenceNumber)` en el eventLog de Capa 2 + key del topic Pulsar de salida = `bet_reference` → reentregas seguras.
2. **Atomicidad de enrichment**: una bet emite al sink sólo cuando `enrichment == SUCCESS`; si los lookups fallan, va a `dlq.enrichment-failed` o queda en retry buffer.
3. **Corrección temporal**: el lookup de todas las dimensiones globales (sport, tournament, market-def, fixture, market, competitor) usa `VersionLookup.floor(list, ce-time)` (búsqueda binaria sobre `List` ordenada por `validFromMs`) → la bet ve la oferta vigente al momento del placement, no la actual.
4. **Resiliencia a out-of-order**: event sourcing en Capa 2 (eventLog + replay) tolera eventos late sin importar cuánto se retrasen, mientras la bet siga en state (TTL 90 días por defecto).
5. **Schema evolution**: POJOs con campos discriminadores + `PojoSerializer`/`AvroSerializer` permiten añadir nuevos tipos de evento, dimensiones de catálogo o campos sin invalidar el savepoint (ver §6.4.1 y §6.4.8).
6. **Recuperabilidad**: state checkpointed (RocksDB + S3 incremental); broadcast state se reconstruye replayando catalog topics compactados desde el principio.
7. **Observabilidad**: side outputs estructurados (DLQs en Pulsar), métricas via reporter, sin watermark lag (no usamos watermarks).

---

## 10. Extensibilidad

### Nuevo tipo de evento de bet
1. Añadir nuevo campo opcional `xPayload` al final del POJO `BetEvent` y un nuevo POJO `XPayload`.
2. Mapear el nuevo topic en Capa 1.
3. Definir transiciones válidas en la state machine (§7.1).
4. Añadir el case en el dispatcher de Capa 2 (§5.3 y §6.3 Capa 2).
5. Compatible con savepoint anterior porque el campo es opcional.

### Nueva dimensión GLOBAL
1. Definir un nuevo POJO `XVersion`.
2. Añadir campo opcional `x` (de tipo `XVersion`) y entrada `catalogType="X"` al POJO `CatalogUpdate`.
3. Añadir un nuevo `MapStateDescriptor<String, List<XVersion>>` en el enricher (manteniendo invariante de orden por `validFromMs`).
4. Conectar el nuevo topic Pulsar como source y unionarlo al stream broadcast.
5. Añadir resolución vía `VersionLookup.floor()` en `tryEnrich()` del enricher.
6. Compatible con savepoint anterior por el mismo motivo.

### Nueva dimensión ESPECÍFICA
1. Añadir Caffeine cache en el enricher.
2. Implementar repo JDBC contra StarRocks dim.
3. Wire lookup en `tryEnrich()` con stampede control.

### "God enricher"
Si crecen >6 dimensiones, particionar:

```
BetEvent ──► OfferEnricher (fixture+market+competitor+globals) ──► PunterEnricher ──► RiskEnricher
```

Cada uno con su `keyBy(betReference)` + broadcast propio.

---

## 11. Datos faltantes / pendientes de clarificación

> Esta sección recoge los campos, comportamientos y contratos que no quedaron claros en la documentación analizada y deben confirmarse con los equipos respectivos antes de implementación.

### Eventos
1. **Topics Pulsar para Bet Placement events**: los PDFs solo mencionan explícitamente `persistent://sportsbook/betting/settlement-events` para settlement. **No se especifica** el topic (o si son varios topics o uno único) para `BetCaptured`/`Accepted`/`Placed`/`Rejected`/`Failed`/`Rollback`. Confirmar si:
   - Cada tipo va a un topic distinto, o
   - Todos van a un único topic `placement-events` con discriminación por `ce-type`.
2. **`sequenceNumber` monotónico por bet**: la propuesta arquitectónica original lo asume, pero los esquemas actuales no lo incluyen. ¿Lo puede emitir el producer? Si no, ¿es aceptable derivarlo del par `(eventTypeOrder, ce-time)` o se necesita un campo explícito?&lt;br&gt;**Impacto si se añade**: se podría reactivar un DLQ adicional `dlq.sequence-gaps` para detectar pérdidas reales en el transporte Pulsar (gap entre seqs contiguos). Hoy esta detección no es posible y los drops del producer caen en `dlq.invalid-transitions / MISSING_PREREQUISITE` mezclados con otros casos (§6.5).
3. **Otros eventos de settlement**: el doc `History_ Data Modeling` dice "there are others bet settlement events being defined" y el PDF Integration confirma "contracts are pending to be defined". ¿Cuáles son? Posibles candidatos: `LegCorrected` (mencionado en `LegGraded.description`), `SettlementRolledBack` (mencionado en `BetSettled.previousSettlementId`).
4. **Cardinalidad real**: `bet.legs[].selections[]` ¿siempre tiene 1 elemento por leg en placement actuales, o ya hay SGP/Composite con N selections? El esquema lo permite pero los ejemplos solo muestran 1.
5. **Mapeo `outcome` (int) → estado**: en `BetGraded.outcome`, `LegGraded.outcome`, `BetSettled.outcome` los valores son enteros (`0=Pending, 1=Won/Cancelled, 2=Lost/Won, ...`). El PDF muestra mapeos inconsistentes (ej. `BetSettled` documenta `1=Won` pero `BetGraded` documenta `1=Cancelled`). Confirmar el enum oficial de `GradingResult` vs `SettlementResult`.
6. **`walletOperation`**: en el ejemplo de `EnrichedBet` aparece `"walletOperation": "SettleWon"`, pero en el evento `BetSettled.wallet.operation` la doc dice "WalletOperationType enum name" sin enumerar. Confirmar valores: `SettlementWon`, `SettlementLost`, `SettlementVoid`, etc.

### Catálogos
7. **Topics de catálogo y formato**: los esquemas `Market`, `Fixture/Match/Race/Outright`, `Sport` están en el PDF Integration, pero no se especifica:
   - El topic Pulsar exacto (¿`persistent://offer/catalog/markets`?, ¿uno por entidad?).
   - Si los eventos son CDC (insert/update/delete) o snapshot-only.
   - Si hay un campo `validFrom`/`version` en el wrapper de catálogo o solo dentro de `odds.version`.
8. **Tournaments y Competitors**: aparecen en el modelo `EnrichedBet` (`tournamentId`, `tournamentName`, `competitors[].id`, `competitors[].name`) pero no hay esquema en `History_ Data Integration` para estas entidades. ¿Vienen embebidas en `Fixture`? ¿Se publican en su propio topic?
9. **Punter / Players**: el `EnrichedBet` tiene un bloque `punter.{id, limits}` pero no hay esquema de punter en los docs ni topic de CDC mencionado. ¿Lookup contra qué dimensión?
10. **`offeringId`**: aparece en todos los eventos como integer, pero no se enriquece nada con él en el ejemplo. ¿Hay un catálogo de offerings? ¿Marketplace/skin/jurisdicción?

### Modelo de salida
11. **Diferencia `BetGraded` → `BetSettled` en `betStatus`**: la tabla del PNG sugiere que `BetGraded` solo modifica `bet.gradeStatus` y `BetSettled` modifica `bet.betStatus = Settled`. ¿Es así, o un bet `Graded + walletOperation=NoAction` también pasa a `Settled` sin esperar el evento `BetSettled`?
12. **Campo `rollback`**: no aparece en el ejemplo del JSON `EnrichedBet` del PDF Data Modeling. ¿Dónde se ubica el detalle del rollback? Asumido en `bet.rollback`, confirmar.
13. **Persistencia de `validationTrail`**: ¿se conserva en el EnrichedBet o solo se usa para metadata interna del enricher?
14. **`liveDelay`**: presente en `BetPlaced` pero no aparece en el `EnrichedBet` ejemplo. Confirmar si se persiste.

### Operacionales
15. **Volúmenes esperados**: throughput de placement (req/s pico), volumen diario de settlement, número de bets activas concurrentes, retention.
16. **SLA de latencia**: ¿qué latencia event-to-EnrichedBet pretende ofrecer el ticker? (sub-segundo, <2 s, <5 s?)
17. **Política de retención en StarRocks** y de DLQ Pulsar (compactado vs retención por tiempo).
18. **Modo de despliegue de StarRocks**: ¿shared-storage o shared-nothing? ¿Cluster compartido con el data warehouse o dedicado al ticker?
19. **Decisión final del PK del sink**: `bet_reference` vs `ticket_id`. Recomendado el primero (ver §5.4).

---

## 12. Diagramas

Los diagramas se publican en `bet-ticker-enricher-diagrams.drawio` en este mismo directorio (6 hojas).

| # | Diagrama | Tipo | Foco |
|---|---|---|---|
| 1 | **System Context** | C4-L1 | Personas, sistemas externos, alcance del Bet Ticker Enricher |
| 2 | **Containers** | C4-L2 | Topics Pulsar, Flink Job, RocksDB, S3, StarRocks, UI, DLQs |
| 3 | **Components (4 capas)** | C4-L3 | Detalle de operadores y state por capa (Ingestion → Status Manager → Enricher → Sink) |
| 4 | **Job Graph (4 capas × 10 eventos)** | Data flow | 10 sources + catálogos en swim lanes por capa |
| 5 | **Bet Lifecycle State Machine** | UML state | Transiciones válidas / terminales (Requested → Settled) |
| 6 | **EnrichedBet / BetProjection** | UML class | Modelos compartidos entre capas (§6.4) + catálogos + state interno |
