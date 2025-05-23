use chrono::{Datelike, Days, NaiveDate, Weekday};
use polars::prelude::*;

fn create_sales_data() -> DataFrame {
    let from_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let _end_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    // We sell something on weekdays only, no weekends (and for simplicity, there are no holidays)
    let mut weekdays_in_2023 = vec![];
    for day_of_year in 0..365 {
        let d = from_date.checked_add_days(Days::new(day_of_year)).unwrap();
        match d.weekday() {
            Weekday::Sat | Weekday::Sun => { /* skip weekends */ }
            _ => weekdays_in_2023.push(d),
        }
    }

    let product_names =
        StringChunked::from_slice("product_names".into(), &["Coffee", "Tea", "Cake"]);

    const SALES_PER_DAY: usize = 10;
    let line_items = weekdays_in_2023.len() * SALES_PER_DAY;
    let mut date_lines = Vec::with_capacity(line_items);
    let mut price_lines = Vec::with_capacity(line_items);

    let qty_dist = Series::new("qtys".into(), 1..11.into());
    // with_replacement allows us to sample more values than there are in the distribution
    // casting to ChunkedArray gives us a typed array that makes the following code simpler (Series is of type AnyValue)
    let quantity_series = qty_dist
        .sample_n(line_items, true, true, None)
        .unwrap()
        .i32()
        .unwrap()
        .clone();
    let product_series = product_names
        .sample_n(line_items, true, true, None)
        .unwrap()
        .as_string()
        .clone();

    let mut i = 0;
    for d in weekdays_in_2023 {
        for _q in 1..=SALES_PER_DAY {
            date_lines.push(d);
            let p = product_series.get(i).unwrap();
            let qty = quantity_series.get(i).unwrap();
            let price = match p {
                "Coffee" => 35 * qty,
                "Tea" => 39 * qty,
                "Cake" => 45 * qty,
                _ => panic!("Unknown product"),
            } as u32;
            price_lines.push(price);
            i += 1;
        }
    }

    DataFrame::new(vec![
        Series::new("Date".into(), date_lines).into(),
        Series::new("Product".into(), product_series.into_series()).into(),
        Series::new("Quantity".into(), quantity_series.into_series()).into(),
        Series::new("Price".into(), price_lines).into(),
    ])
    .unwrap()
}

fn sales_report() {
    let df = create_sales_data();

    println!(
        "Total sales: {}",
        df.column("Price").unwrap().u32().unwrap().sum().unwrap()
    );

    let sales_by_product = df
        .clone()
        .lazy()
        .select(vec![col("*")])
        .group_by(vec![col("Product")])
        .agg(vec![col("Price").sum()])
        .collect();
    println!("Sales by product: {:?}", sales_by_product);

    let opts = DynamicGroupOptions {
        // Every calendar month
        // "mo": calendar month
        // every: indicates the interval of the window
        every: Duration::parse("1mo"),
        // Window length is 1 calendar month
        // period: indicates the duration of the window
        period: Duration::parse("1mo"),
        // Start at start of month
        // If you merge the above calendar months with the Default::default() from
        // below without setting this to a time-like value, you will get panic
        offset: Duration::parse("0d"),
        // Align window to start of month, not the data points
        // this is the default: truncate: true,
        ..Default::default()
    };

    let sales_by_calendar_month = df
        .clone()
        .lazy()
        // We can mark the data frame as sorted, or sort it explicitly -
        // or we will get an error when calling groupby_dynamic
        .sort(["Date"], SortMultipleOptions::default())
        .group_by_dynamic(col("Date"), vec![], opts.clone())
        .agg(vec![col("Price").sum()])
        .collect();
    println!("Sales by month: {:?}", sales_by_calendar_month);

    let sales_by_product_by_calendar_month = df
        .clone()
        .lazy()
        .sort(["Date"], SortMultipleOptions::default())
        // Putting Product in the second arg passes it through
        .group_by_dynamic(col("Date"), vec![col("Product")], opts.clone())
        .agg(vec![col("Price").sum()])
        // We can reorder the columns and sort to our liking like this
        .select(vec![col("Date"), col("Product"), col("Price")])
        .sort(["Date"], SortMultipleOptions::default())
        .collect()
        .unwrap();
    println!(
        "Sales by product and month: {}",
        sales_by_product_by_calendar_month
    );
}

/// Working with and without time-zones and converting between them.
fn time_zones() -> PolarsResult<DataFrame> {
    let ts_local = [
        "2025-05-22T10:00:00",
        "2025-05-22T11:00:00",
        "2025-05-23T12:00:00",
    ];
    // Naive in chrono means with no timezone
    let tz_naive = Column::new("tz_naive".into(), &ts_local);

    DataFrame::new(vec![tz_naive.into()])
        .unwrap()
        .lazy()
        // Enable the strings and dtype-datetime features for this:
        .select(vec![col("tz_naive").str().to_datetime(
            Some(TimeUnit::Milliseconds),
            None,
            StrptimeOptions::default(),
            lit("raise"),
        )])
        .with_columns([col("tz_naive")
            .dt()
            .replace_time_zone(
                // The TimeZone in the polars::prelude module has the feature we need
                TimeZone::opt_try_new(Some("Europe/Copenhagen")).unwrap(),
                lit("raise"),
                NonExistent::Raise,
            )
            .alias("tz_copenhagen")])
        .with_columns([col("tz_copenhagen")
            .dt()
            .convert_time_zone(TimeZone::UTC)
            .alias("tz_utc")])
        .collect()
}

fn main() {
    println!("Starting...");
    sales_report();
    println!();
    println!("Time zones");
    println!("{}", time_zones().unwrap());
    println!("Done.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_zones() {
        let df = time_zones().unwrap();

        println!("df: {:?}", df);

        assert_eq!((3, 3), df.shape());
        assert_eq!(
            vec!["tz_naive", "tz_copenhagen", "tz_utc"],
            df.get_column_names_str()
        );
    }
}
