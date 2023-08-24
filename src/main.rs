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
            _ => weekdays_in_2023.push(d),
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
    let quantity_series = qty_dist
        .sample_n(line_items, true, true, None)
        .unwrap()
        .i32()
        .unwrap()
        .clone();
    let product_series = product_names
        .sample_n(line_items, true, true, None)
        .unwrap()
        .as_utf8()
        .clone();

    let mut i = 0;
    for d in weekdays_in_2023 {
        for q in 1..=SALES_PER_DAY {
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
        Series::new("Date", date_lines),
        Series::new("Product", product_series.into_series()),
        Series::new("Quantity", quantity_series.into_series()),
        Series::new("Price", price_lines),
    ])
    .unwrap()
}

fn sales_report() {
    let df = create_sales_data();

    println!(
        "Total sales: {}",
        df.column("Price").unwrap().sum::<u32>().unwrap()
    );

    let sales_by_product = df
        .clone()
        .lazy()
        .select(vec![col("*")])
        .groupby(vec![col("Product")])
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
        // If you merge the above calendard months with the Default::default() from
        // below without setting this to a time-like value, you will get panic
        offset: Duration::parse("0d"),
        // Align window to start of month, not the data points
        truncate: true,
        ..Default::default()
    };

    let sales_by_calendar_month = df
        .clone()
        .lazy()
        // We can mark the data frame as sorted, or sort it explicitly -
        // or we will get an error when calling groupby_dynamic
        .sort("Date", SortOptions::default())
        .groupby_dynamic(col("Date"), vec![], opts.clone())
        .agg(vec![col("Price").sum()])
        .collect();
    println!("Sales by month: {:?}", sales_by_calendar_month);

    let sales_by_product_by_calendar_month = df
        .clone()
        .lazy()
        .sort("Date", SortOptions::default())
        // Putting Product in the second arg passes it through
        .groupby_dynamic(col("Date"), vec![col("Product")], opts.clone())
        .agg(vec![col("Price").sum()])
        // We can reorder the columns and sort to our liking like this
        .select(vec![col("Date"), col("Product"), col("Price")])
        .sort("Date", SortOptions::default())
        .collect()
        .unwrap();
    println!(
        "Sales by product and month: {}",
        sales_by_product_by_calendar_month
    );
}

fn main() {
    println!("Starting...");
    sales_report();
    println!("Done.")
}
