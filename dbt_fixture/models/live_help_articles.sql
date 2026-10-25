-- Consumers read silver WHERE NOT is_deleted (PLAN G3).
select id, title, lsn
from {{ ref('help_articles_fixture') }}
where not is_deleted
