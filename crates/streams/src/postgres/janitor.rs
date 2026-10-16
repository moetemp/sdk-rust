//! The janitor keeps retention, since PostgreSQL has no key expiry. Every store runs one, and a
//! session advisory lock lets one pass run at a time across processes.

use super::{
    DELETE_BATCH, Inner, LOG_CAP,
    errors::{Call, mapped, timed_out, unreachable},
    migrations::LOCK_SPACE,
};
use crate::StreamResult;
use std::{sync::Weak, time::Duration};
use tokio_postgres::{Client, Row, types::ToSql};

/// How many times one pass trims a topic before it moves to the next one.
const TRIMS_PER_TOPIC: usize = 100;
/// How many idle chains one pass deletes at most.
const IDLE_CHAINS: i32 = 500;

/// Runs a pass every `interval` until the store goes.
pub(super) async fn run(inner: Weak<Inner>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if let Err(error) = inner.janitor_pass().await {
            tracing::warn!("The PostgreSQL stream store's janitor pass failed: {error}");
        }
    }
}

impl Inner {
    /// One pass: trims the topics that are due, finishes purges, caps the notification log, and
    /// deletes idle chains.
    pub(super) async fn janitor_pass(&self) -> StreamResult<u64> {
        let client = self.pool.get().await.map_err(unreachable)?;
        let key = format!("{}:janitor", self.schema);
        let locked = self
            .on(
                &client,
                "SELECT pg_try_advisory_lock($1, hashtext($2))",
                &[&LOCK_SPACE, &key],
            )
            .await?;
        if !locked.first().is_some_and(|row| row.get::<_, bool>(0)) {
            return Ok(0);
        }
        let passed = self.janitor_steps(&client).await;
        let unlocked = match &passed {
            Ok(_) => self
                .on(
                    &client,
                    "SELECT pg_advisory_unlock($1, hashtext($2))",
                    &[&LOCK_SPACE, &key],
                )
                .await
                .map(drop),
            Err(_) => Ok(()),
        };
        if passed.is_err() || unlocked.is_err() {
            // Closing the connection ends its session lock.
            drop(deadpool_postgres::Object::take(client));
        }
        unlocked?;
        passed
    }

    async fn janitor_steps(&self, client: &Client) -> StreamResult<u64> {
        let mut deleted = 0;
        for row in self.on(client, &self.sql.due, &[]).await? {
            let topic: i64 = row.get(0);
            for _ in 0..TRIMS_PER_TOPIC {
                let trimmed = self
                    .on(
                        client,
                        &self.sql.trim,
                        &[&topic, &self.retention_ms, &self.grace_ms, &DELETE_BATCH],
                    )
                    .await?;
                let count: i64 = trimmed[0].get(0);
                deleted += count as u64;
                if trimmed[0].get::<_, bool>(1) || count == 0 {
                    break;
                }
            }
        }
        loop {
            let purged = self.on(client, &self.sql.purge, &[&DELETE_BATCH]).await?;
            deleted += purged[0].get::<_, i64>(0) as u64;
            if purged[0].get::<_, bool>(1) {
                break;
            }
        }
        if self.log {
            self.on(client, &self.sql.cap_log, &[&LOG_CAP]).await?;
        }
        self.on(
            client,
            &self.sql.idle_chains,
            &[&self.retention_ms, &self.grace_ms, &IDLE_CHAINS],
        )
        .await?;
        Ok(deleted)
    }

    async fn on(
        &self,
        client: &Client,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> StreamResult<Vec<Row>> {
        match tokio::time::timeout(self.call_timeout, client.query(sql, params)).await {
            Ok(answer) => answer.map_err(|error| mapped(Call::Write, &error)),
            Err(_) => Err(timed_out(Call::Write)),
        }
    }
}
