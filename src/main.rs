use std::{fs, io::Read, path::Path};

use clap::{Parser, Subcommand};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
    #[arg(short, long, default_value = "migration.conf")]
    config: String,
    #[arg(short, long, default_value = "migrations")]
    migrations_dir: String,
    #[arg(short, long)]
    db: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run,
    Revert,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut run_migrations: Vec<&str> = Vec::with_capacity(16);
    let mut revert_migrations: Vec<&str> = Vec::with_capacity(16);
    let mut after_seperator = false;
    let file = std::fs::read_to_string(&args.config).expect("could not find config file");
    for line in file.lines() {
        let line = line.trim();
        if line.is_empty() {
            if after_seperator == false {
                after_seperator = true;
            } else {
                break;
            }
            continue;
        }

        if after_seperator {
            revert_migrations.push(line);
        } else {
            run_migrations.push(line);
        }
    }
    println!("run migrations: {run_migrations:?}");
    println!("revert migrations: {revert_migrations:?}");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&args.db)
        .await
        .expect("could not connect to db");
    println!("connected to the db successfully");

    match args.cmd {
        Commands::Run => {
            migrate_all(
                &pool,
                "up.sql",
                Path::new(&args.migrations_dir),
                run_migrations,
            )
            .await
            .expect("could not execute run migration");
        }
        Commands::Revert => {
            migrate_all(
                &pool,
                "down.sql",
                Path::new(&args.migrations_dir),
                revert_migrations,
            )
            .await
            .expect("could not execute revert migration");
        }
    }
}

async fn migrate_all(
    db: &Pool<Postgres>,
    filename: &str,
    dir: &Path,
    migrations: Vec<&str>,
) -> Result<(), sqlx::Error> {
    println!("attempting migration");

    sqlx::query::<Postgres>(
        r#"
        CREATE TABLE IF NOT EXISTS _migrations (
        name VARCHAR PRIMARY KEY,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .execute(db)
    .await?;

    for mg in migrations {
        let mut path = dir.join(mg);
        path.push(filename);
        match filename {
            "up.sql" => match run_one(path.as_path(), db).await {
                Ok(()) => println!("up migration successfully done for {}", mg),
                Err(e) => eprintln!("could not run migration for {}, {e:?}", mg),
            },
            "down.sql" => match revert_one(path.as_path(), db).await {
                Ok(()) => println!("down migration successfully done for {}", mg),
                Err(e) => eprintln!("could not run migration for {}, {e:?}", mg),
            },
            &_ => println!("found an unusual file, only use up.sql or down.sql"),
        }
    }

    Ok(())
}

async fn run_one(path: &Path, pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    let name = path
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let mut file = fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut buff = String::new();
    file.read_to_string(&mut buff).unwrap();
    // check if migration exists
    println!("checking if the migration for {path:?} exists");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM _migrations WHERE name = $1)")
            .bind(name)
            .fetch_one(pool)
            .await?;
    if !exists {
        // create
        let mut tx = pool.begin().await?;
        let statements: Vec<&str> = buff
            .split(";")
            .map(|e| e.trim())
            .filter(|e| !e.is_empty())
            .collect();
        for statement in statements {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        // record migration
        sqlx::query("INSERT INTO _migrations (name) VALUES ($1)")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    } else {
        Err(sqlx::Error::InvalidArgument(format!(
            "migration entry for {name} already exists"
        )))
    }
}

async fn revert_one(path: &Path, pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    let name = path
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let mut file = fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut buff = String::new();

    file.read_to_string(&mut buff).unwrap();
    // check if migration exists
    println!("checking if the migration for {path:?} exists");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM _migrations WHERE name = $1)")
            .bind(name)
            .fetch_one(pool)
            .await?;
    if exists {
        // create
        let mut tx = pool.begin().await?;
        let statements: Vec<&str> = buff
            .split(";")
            .map(|e| e.trim())
            .filter(|e| !e.is_empty())
            .collect();
        for statement in statements {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        // record migration
        sqlx::query("DELETE FROM _migrations WHERE name = $1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    } else {
        Err(sqlx::Error::InvalidArgument(format!(
            "migration {name} did not exist"
        )))
    }
}
