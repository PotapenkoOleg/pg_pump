mod clap_parser;
mod config_provider;
mod helpers;
mod sql_server;
mod version;

use crate::clap_parser::Args;
use crate::config_provider::ConfigProvider;
use crate::helpers::{print_banner, print_separator};
use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use std::process;

#[tokio::main]
async fn main() -> Result<()> {
    print_separator();
    print_banner();
    print_separator();
    let args = Args::parse();
    println!("Loading Config File: <{}> ", &args.config_file);
    let config_provider = ConfigProvider::new(&args.config_file);
    let file_load_result = config_provider.read_config().await;
    if file_load_result.is_err() {
        eprintln!("{}", file_load_result.err().unwrap().to_string().red());
        process::exit(1);
    }
    let config = file_load_result.ok().unwrap();
    //println!("{:#?}", config);
    println!("{}", "DONE Loading Config File".green());
    print_separator();

    Ok(())
}
