ATTACH DATABASE 'exampleNoStanza.db' AS noStanza;

CREATE TABLE statements (
      stanza TEXT,
      subject TEXT,
      predicate TEXT,
      object TEXT
    );


WITH RECURSIVE
  stanzas(stanza, subject, predicate, object) AS (
     SELECT subject, subject, predicate, object FROM noStanza.statements 
     UNION ALL
     SELECT s.stanza, no.subject, no.predicate, no.object FROM stanzas s INNER JOIN noStanza.statements no ON s.object = no.subject AND s.object LIKE '\_%' ESCAPE '\'
  )
INSERT INTO statements
SELECT * FROM stanzas ;



