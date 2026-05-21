pub fn database_url_or_skip() -> Result<Option<String>, String> {
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());

    if database_url.is_none() && require_database() {
        Err(
            "DATABASE_URL must be set when APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE is enabled"
                .to_owned(),
        )
    } else {
        Ok(database_url)
    }
}

fn require_database() -> bool {
    matches!(
        std::env::var("APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
    )
}
