-- The ledger takes opening balances from the account's first change event (op 'c'). An account first seen as a
-- snapshot read (op 'r') has an unknown opening balance and is unsupported (ADR-0026); g-checks start from empty volumes.
select account_id, op, lsn
from {{ ref('stg_bronze__account_openings') }}
where op <> 'c'
