ATTACH DATABASE 'exampleNoStanza.db' AS noStanza;

CREATE TABLE statements (
      stanza TEXT,
      subject TEXT,
      predicate TEXT,
      object TEXT
    );


WITH RECURSIVE
  stanza(sta, subject, predicate, object) AS (
     SELECT subject, subject, predicate, object FROM noStanza.statements WHERE NOT subject LIKE '\_%' ESCAPE '\'
     UNION ALL
     SELECT s.sta, no.subject, no.predicate, no.object FROM stanza s INNER JOIN noStanza.statements no ON s.object = no.subject AND s.object LIKE '\_%' ESCAPE '\'
  )
INSERT INTO statements
SELECT * FROM stanza ;



