use crate::clap_parser::{Args, YesNoEnum};
use std::fmt;

pub struct Settings {
    source_schema_name: String,
    source_table_name: String,
    target_schema_name: String,
    target_table_name: String,
    column_name: String,
    min: u64,
    max: u64,
    threads: u32,
    timeout: u64,
    wait_period: u64,
    wait_nth_partition: u32,
    truncate_target_table: bool,
    min_records_per_partition: i64,
}

impl Settings {
    pub fn from_args(args: &Args) -> Self {
        let source_schema_name = args.source_schema.clone();

        let mut source_table_name = args.source_table.clone();
        if source_table_name.eq("*") {
            source_table_name = "*".to_string();
        }

        let mut target_schema_name = args.target_schema.clone();
        if source_schema_name.eq("*") {
            target_schema_name = "$".to_string();
        } else {
            if target_schema_name.eq("$") {
                target_schema_name = source_schema_name.clone();
            }
        }

        let mut target_table_name = args.target_table.clone();
        if source_table_name.eq("*") {
            target_table_name = "$".to_string();
        } else {
            if target_table_name.eq("$") {
                target_table_name = source_table_name.clone();
            }
        }

        let column_name = args.column.clone();

        let mut min = args.min.clone();

        let max = args.max.clone();
        if max == 0 {
            min = 0;
        }

        let threads = args.threads.clone();

        let timeout = args.timeout.clone();

        let wait_period = args.wait_period.clone();

        let wait_nth_partition = args.wait_nth_partition.clone();

        let mut truncate_target_table: bool = false;
        match args.truncate_target_table {
            YesNoEnum::Yes => {
                truncate_target_table = true;
            }
            YesNoEnum::No => {
                truncate_target_table = false;
            }
        }

        let min_records_per_partition = args.min_records_per_partition.clone();

        Settings {
            source_schema_name,
            source_table_name,
            target_schema_name,
            target_table_name,
            column_name,
            min,
            max,
            threads,
            timeout,
            wait_period,
            wait_nth_partition,
            truncate_target_table,
            min_records_per_partition,
        }
    }

    // region Getters
    pub fn get_source_schema_name_as_ref(&self) -> &String {
        &self.source_schema_name
    }

    pub fn get_source_table_name_as_ref(&self) -> &String {
        &self.source_table_name
    }

    pub fn get_target_schema_name_as_ref(&self) -> &String {
        &self.target_schema_name
    }

    pub fn get_target_table_name_as_ref(&self) -> &String {
        &self.target_table_name
    }

    pub fn get_column_name_as_ref(&self) -> &String {
        &self.column_name
    }

    pub fn get_min(&self) -> u64 {
        self.min
    }

    pub fn get_max(&self) -> u64 {
        self.max
    }

    pub fn get_threads(&self) -> u32 {
        self.threads
    }

    pub fn get_timeout(&self) -> u64 {
        self.timeout
    }

    pub fn get_wait_period(&self) -> u64 {
        self.wait_period
    }

    pub fn get_wait_nth_partition(&self) -> u32 {
        self.wait_nth_partition
    }

    pub fn is_truncate_target_table(&self) -> bool {
        self.truncate_target_table
    }

    pub fn get_min_records_per_partition(&self) -> i64 {
        self.min_records_per_partition
    }
    // endregion

    // region Setters
    pub fn set_threads(&mut self, threads: u32) {
        self.threads = threads;
    }
    // endregion
}

impl fmt::Display for Settings {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Source schema name: <{}>", self.source_schema_name)?;
        writeln!(f, "Source table name: <{}>", self.source_table_name)?;
        writeln!(f, "Target schema name: <{}>", self.target_schema_name)?;
        writeln!(f, "Target table name: <{}>", self.target_table_name)?;
        writeln!(f, "Column name: <{}>", self.column_name)?;
        writeln!(f, "Min: <{}>", self.min)?;
        writeln!(f, "Max: <{}>", self.max)?;
        writeln!(f, "Threads: <{}>", self.threads)?;
        writeln!(f, "Timeout: <{}> seconds", self.timeout)?;
        writeln!(f, "Wait period: <{}> seconds", self.wait_period)?;
        writeln!(
            f,
            "Wait after processing n-th partition: <{}>",
            self.wait_nth_partition
        )?;
        writeln!(f, "Truncate target table: <{}>", self.truncate_target_table)?;
        writeln!(
            f,
            "Min records per partition: <{}>",
            self.min_records_per_partition
        )?;
        Ok(())
    }
}
