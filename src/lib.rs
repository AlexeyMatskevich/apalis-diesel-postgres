//! PostgreSQL storage backend for Apalis implemented with Diesel.

/// Returns the crate name.
#[must_use]
pub const fn crate_name() -> &'static str {
    "apalis-diesel-postgres"
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn exposes_crate_name() {
        assert_eq!(crate_name(), "apalis-diesel-postgres");
    }
}
