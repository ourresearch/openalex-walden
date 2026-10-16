-- Object-local recovery: unrelated input drift must not strand the served name.
SELECT assert_true(max(version)=41,'served original changed: STOP') FROM (DESCRIBE HISTORY openalex.funders.funders);
SELECT assert_true(count(*)=1,'original table missing') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders' AND table_type<>'VIEW';
