# ADR 0002: PostgreSQL owns graph and prediction state

Status: Accepted

The product needs bounded one-/two-hop queries, transactions, uniqueness,
inbox/outbox state, and canonical multi-version scores. Indexed source and
destination edge queries satisfy the current graph shape while PostgreSQL gives
the strongest required transaction boundary. A graph database would add
operations and consistency coordination without evidence that these bounded
queries need it.
