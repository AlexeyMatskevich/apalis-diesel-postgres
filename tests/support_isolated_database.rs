//! The isolated-database helper removes its database however the scenario ends.
#![cfg(feature = "tokio")]
mod support;

use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Text},
};
use lets_expect::*;
use std::sync::{Arc, Mutex};
use support::{Outcome, observe};

#[derive(Clone, Copy)]
enum Scenario {
    Completes,
    Panics,
}

#[derive(Debug)]
struct Observation {
    database_remains: bool,
    panicked: bool,
}

/// The database name is the last path segment of the URI handed to the scenario.
fn database_name(url: &str) -> String {
    let path = url.rsplit('/').next().unwrap_or_default();
    path.split('?').next().unwrap_or_default().to_owned()
}

async fn database_exists(maintenance_url: String, name: String) -> Result<bool, String> {
    tokio::task::spawn_blocking(move || {
        #[derive(QueryableByName)]
        struct Count {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        let mut conn = PgConnection::establish(&maintenance_url).map_err(|e| e.to_string())?;
        sql_query("SELECT count(*)::bigint AS n FROM pg_database WHERE datname = $1")
            .bind::<Text, _>(name)
            .get_result::<Count>(&mut conn)
            .map(|row| row.n > 0)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn isolated_scenario(scenario: Scenario) -> Result<Outcome<Observation>, String> {
    let Some(maintenance_url) = support::database_url_or_skip()? else {
        return Ok(Outcome::Skipped);
    };
    let created = Arc::new(Mutex::new(None::<String>));
    let record = created.clone();
    let handle = tokio::spawn(async move {
        support::with_isolated_database(|url| async move {
            *record.lock().unwrap() = Some(database_name(&url));
            if matches!(scenario, Scenario::Panics) {
                panic!("the scenario panics after creating its database");
            }
            Ok::<(), String>(())
        })
        .await
    });
    let panicked = match handle.await {
        Ok(result) => {
            result?;
            false
        }
        Err(join) if join.is_panic() => true,
        Err(join) => return Err(join.to_string()),
    };
    let name = created
        .lock()
        .unwrap()
        .clone()
        .ok_or("the scenario never received its database")?;
    Ok(Outcome::Completed(Observation {
        database_remains: database_exists(maintenance_url, name).await?,
        panicked,
    }))
}

fn removed_its_database(
    panicked: bool,
) -> impl Fn(&Result<Outcome<Observation>, String>) -> AssertionResult {
    observe("isolated database lifecycle", move |o: &Observation| {
        if !o.database_remains && o.panicked == panicked {
            Ok(())
        } else {
            Err(format!(
                "expected no remaining database and panicked={panicked}, got {o:?}"
            ))
        }
    })
}

lets_expect! { #tokio_test
    expect(isolated_scenario(scenario).await) as an_isolated_database_scenario {
        let scenario = Scenario::Completes;
        to removes_its_database { removed_its_database(false) }
        when the_scenario_panics {
            let scenario = Scenario::Panics;
            to removes_its_database_and_propagates_the_panic { removed_its_database(true) }
        }
    }
}
