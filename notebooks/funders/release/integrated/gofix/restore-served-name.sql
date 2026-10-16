-- Object-local recovery: unrelated input drift must not strand the served name.
SELECT assert_true(max(version)=41,'retained original advanced: STOP') FROM (DESCRIBE HISTORY openalex.funders.funders_v41_table);
SELECT assert_true(count(*)=0,'retained original bytes changed: STOP') FROM ((SELECT * FROM openalex.funders.funders_v41_table EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_clone) UNION ALL (SELECT * FROM openalex.funders.funders_v41_clone EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_table));
SELECT assert_true(count(*)=0,'compat already activated: full rollback required') FROM ((SELECT * FROM openalex.funders.funders_compat EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_clone) UNION ALL (SELECT * FROM openalex.funders.funders_v41_clone EXCEPT ALL SELECT * FROM openalex.funders.funders_compat));
SELECT assert_true(count(*)=0,'served relation exists: use view recovery') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders';
ALTER TABLE openalex.funders.funders_v41_table RENAME TO openalex.funders.funders;
SELECT assert_true(max(version)=41,'served Delta history changed') FROM (DESCRIBE HISTORY openalex.funders.funders);
SELECT assert_true(count(*)=0,'restored bytes differ') FROM ((SELECT * FROM openalex.funders.funders EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_clone) UNION ALL (SELECT * FROM openalex.funders.funders_v41_clone EXCEPT ALL SELECT * FROM openalex.funders.funders));
