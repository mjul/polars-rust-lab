use polars::export::chrono::{Datelike, Days, NaiveDate, Weekday};
use polars::prelude::*;

fn create_sales_data() -> DataFrame {
    let from_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    // We sell something on weekdays only, no weekends (and for simplicity, there are no holidays)
    let mut weekdays_in_2023 = vec![];
    for day_of_year in 0..365 {
        let d = from_date.checked_add_days(Days::new(day_of_year)).unwrap();
        match d.weekday() {
            Weekday::Sat | Weekday::Sun => { /* skip weekends */ }
            _ => weekdays_in_2023.push(d)
        }
    }

    let product_names = Utf8Chunked::from_slice("product_names", &["Coffee", "Tea", "Cake"]);

    const SALES_PER_DAY: usize = 10;
    let line_items = weekdays_in_2023.len() * SALES_PER_DAY;
    let mut date_lines = Vec::with_capacity(line_items);
    let mut price_lines = Vec::with_capacity(line_items);

    let qty_dist = Series::new("qtys", 1..11.into());
    // with_replacement allows us to sample more values than there are in the distribution
    // casting to ChunkedArray gives us a typed array that makes the following code simpler (Series is of type AnyValue)
    let quantity_series = qty_dist.sample_n(line_items, true, true, None).unwrap().i32().unwrap().clone();
    let product_series = product_names.sample_n(line_items, true, true, None).unwrap().as_utf8().clone();

    let mut i = 0;
    for d in weekdays_in_2023 {
        for q in 1..=SALES_PER_DAY {
            date_lines.push(d);
            let p = product_series.get(0).unwrap();
            let qty = quantity_series.get(0).unwrap();
            let price = match p {
                "Coffee" => 35 * qty,
                "Tea" => 39 * qty,
                "Cake" => 45 * qty,
                _ => panic!("Unknown product")
            } as u32;
            price_lines.push(price);
            i += 1;
        }
    }

    DataFrame::new(vec![
        Series::new("Date", date_lines),
        Series::new("Product", product_series.into_series()),
        Series::new("Quantity", quantity_series.into_series()),
        Series::new("Price", price_lines),
    ])
        .unwrap()
}

fn sales_report() {
    let df = create_sales_data();
    println!("Total sales: {}", df.column("Price").unwrap().sum::<u32>().unwrap());
}


fn main() {
    println!("Starting...");
    sales_report();
    println!("Done.")
}
