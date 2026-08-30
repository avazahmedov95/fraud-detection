// Load the account population into Neo4j.
// Relationship edges between account holders were removed from the design: the
// kinship signal they carried (is_family) was an artefact of how the synthetic
// data was built, not a real one. The graph now serves the money-flow network
// written by the sink (mule fan-in/out, transfer rings).
// CSVs are mounted read-only at the Neo4j import dir from data-generator/out.
// Run with:  make load-graph   (after `make generate`)

// Constraint for fast MERGE / lookups.
CREATE CONSTRAINT person_pinfl IF NOT EXISTS
FOR (p:Person) REQUIRE p.pinfl IS UNIQUE;

// Accounts.
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row
MERGE (p:Person {pinfl: row.pinfl})
  SET p.card             = row.card,
      p.network          = row.network,
      p.full_name        = row.full_name,
      p.bank_code        = row.bank_code,
      p.bank_name        = row.bank_name,
      p.region           = row.region,
      p.account_age_days = toInteger(row.account_age_days),
      p.is_fraud_account = (row.is_fraud_account = 'True');

// Sanity counts.
MATCH (p:Person) RETURN count(p) AS persons;
