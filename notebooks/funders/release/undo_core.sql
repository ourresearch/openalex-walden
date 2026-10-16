-- Superseded by integrated/ROLLBACK.sql.
-- Fail closed: this earlier inverse cannot target the post-cutover served view.
SELECT raise_error('Use reviewed integrated/ROLLBACK.sql before readers resume; after-night rollback is not certified');
