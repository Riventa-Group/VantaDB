use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::Helper;
use rustyline::Context;

/// Tab completer for VantaDB shell commands.
pub struct VantaCompleter {
    commands: Vec<String>,
    pub databases: Vec<String>,
    pub collections: Vec<String>,
}

impl VantaCompleter {
    pub fn new() -> Self {
        let commands = vec![
            "help", "exit", "quit", "clear", "whoami", "use", "show", "create", "drop",
            "insert", "find", "delete", "count", "update", "query", "aggregate", "agg",
            "schema", "begin", "commit", "rollback", "tx", "users", "adduser", "rmuser",
            "passwd", "status", "benchmark", "bench", "acl", "certs", "audit", "metrics",
            "health", "backup", "explain",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        Self {
            commands,
            databases: Vec::new(),
            collections: Vec::new(),
        }
    }

    pub fn update_databases(&mut self, dbs: Vec<String>) {
        self.databases = dbs;
    }

    pub fn update_collections(&mut self, cols: Vec<String>) {
        self.collections = cols;
    }
}

impl Completer for VantaCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line_to_cursor = &line[..pos];
        let parts: Vec<&str> = line_to_cursor.split_whitespace().collect();

        // Complete command name
        if parts.len() <= 1 {
            let prefix = parts.first().copied().unwrap_or("");
            let matches: Vec<Pair> = self
                .commands
                .iter()
                .filter(|cmd| cmd.starts_with(prefix))
                .map(|cmd| Pair {
                    display: cmd.clone(),
                    replacement: cmd.clone(),
                })
                .collect();
            let start = pos - prefix.len();
            return Ok((start, matches));
        }

        // Complete database names after "use"
        if parts[0] == "use" && parts.len() == 2 {
            let prefix = parts[1];
            let matches: Vec<Pair> = self
                .databases
                .iter()
                .filter(|db| db.starts_with(prefix))
                .map(|db| Pair {
                    display: db.clone(),
                    replacement: db.clone(),
                })
                .collect();
            let start = pos - prefix.len();
            return Ok((start, matches));
        }

        // Complete collection names for commands that take them
        let col_commands = ["find", "insert", "delete", "count", "update", "query",
                           "aggregate", "agg", "explain", "schema"];
        if col_commands.contains(&parts[0]) && parts.len() == 2 {
            let prefix = parts[1];
            let matches: Vec<Pair> = self
                .collections
                .iter()
                .filter(|col| col.starts_with(prefix))
                .map(|col| Pair {
                    display: col.clone(),
                    replacement: col.clone(),
                })
                .collect();
            let start = pos - prefix.len();
            return Ok((start, matches));
        }

        Ok((pos, Vec::new()))
    }
}

impl Hinter for VantaCompleter {
    type Hint = String;
}

impl Highlighter for VantaCompleter {}
impl Validator for VantaCompleter {}
impl Helper for VantaCompleter {}
