# gitlab_events_to_clickhouse

clickhouse scheme:
```
CREATE TABLE gitlab_events (
    username String,
    action_name String,
    created_at DateTime('UTC')
) ENGINE = MergeTree
ORDER BY (username, created_at);

```

clickhouse play select:

```
SELECT 
    rowNumberInAllBlocks() AS rank,
    t.*
FROM (
    SELECT
        name,
        sumIf(count, action = 'pushed new') AS pushed_new,
        sumIf(count, action = 'pushed to') AS pushed_to,
        sumIf(count, action = 'approved') AS approved,
        sumIf(count, action = 'accepted') AS accepted,
        sumIf(count, action = 'opened') AS opened,
        sumIf(count, action = 'deleted') AS deleted,
        sumIf(count, action = 'commented on') AS commented_on,
        sumIf(count, action = 'closed') AS closed,
        // sumIf(count, action = 'joined') AS joined,
        sumIf(count, action = 'created') AS created,
        // sumIf(count, action = 'left') AS left,
        // sumIf(count, action = 'removed due to membership expiration from') AS removed_membership_expiration,
        // sumIf(count, action = 'imported') AS imported,
        // sumIf(count, action = 'updated') AS updated,
        sum(count) AS total
    FROM (
        SELECT
            name,
            action,
            count(*) AS count
        FROM gitlab_events
        WHERE name IN (
'user1', 
'user1',
)
        AND date BETWEEN '2023-02-06' AND '2024-02-06'
        GROUP BY name, action
    )
    GROUP BY name
    ORDER BY total DESC
) t

```
<img width="830" alt="screen play" src="https://github.com/foxit337/gitlab_events_to_clickhouse/assets/48482347/122c153e-607c-4b9d-a2d3-632dc223233f">


