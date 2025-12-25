use crate::version::{
    COPYRIGHT, COPYRIGHT_YEARS, LICENSE, LINK, PRODUCT_NAME, VERSION_ALIAS, VERSION_MAJOR,
    VERSION_MINOR, VERSION_PATCH,
};

pub fn print_separator() {
    println!("{}", "*".repeat(80));
}

pub fn print_banner() {
    println!(
        "{}",
        format!(
            "{} version {}.{}.{} ({})",
            PRODUCT_NAME, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH, VERSION_ALIAS
        )
    );
    println!("License: {}", LICENSE);
    println!("Link: {}", LINK);
    println!("Copyright © {}. {}", COPYRIGHT, COPYRIGHT_YEARS);
}
