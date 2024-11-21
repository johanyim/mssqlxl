use std::{borrow::Cow, fmt::Debug};

use futures::TryStreamExt;
use rust_xlsxwriter::Workbook;
use tiberius::{AuthMethod, Client, ColumnData, Config, QueryItem, QueryStream};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let query = "SELECT * from claims_claims";

    let mut config = Config::new();
    config.host("bga-azure-1.database.windows.net");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("tchowdhury", "BGATah123!"));
    config.database("BGA_AZUREDB_1");
    // .database("database")
    config.trust_cert(); // on production, it is not a good idea to do this

    let tcp = TcpStream::connect(config.get_addr()).await?;
    let mut client = Client::connect(config, tcp.compat_write()).await?;
    let mut stream = client.simple_query(query).await?;

    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    let mut row_index = 0;

    while let Some(row) = stream.try_next().await? {
        if row_index == 0 {
            // Write column headers for the first row
            for (col_index, column) in row.as_metadata().unwrap().columns().iter().enumerate() {
                worksheet.write_string(0, col_index as u16, column.name())?;
            }
            row_index += 1;
        }
        fn s_or_null<T: ToString>(t: Option<T>) -> String {
            t.map(|v| v.to_string()).unwrap_or("NULL".to_string())
        }
        macro_rules! arms {
            ($matcher:ident, [$( $Variant:ident), *], $fallback:expr) => {
                match $matcher {
                    // $(ColumnData::$Variant(val) => val.map(|v| v.to_string()).unwrap_or("NULL".to_string()), )*
                    $(ColumnData::$Variant(val) => s_or_null(val), )*
                    _ => $fallback,

                }
            };
        }

        if let Some(row_data) = row.into_row() {
            for (col_index, cell) in row_data.into_iter().enumerate() {
                // let s = arms!(
                //     cell,
                //     [U8, I16, I32, I64, F32, F64, Bit, String, Guid, Numeric, Xml],
                //     "Unsupported".to_string()
                // );

                match cell {
                    ColumnData::DateTime(s) => println!("{:?}", s),
                    ColumnData::DateTime2(s) => println!("{:?}", s),
                    ColumnData::SmallDateTime(s) => println!("{:?}", s),
                    ColumnData::DateTimeOffset(s) => println!("{:?}", s),
                    ColumnData::Time(s) => println!("{:?}", s),
                    ColumnData::Date(s) => println!("{:?}, days = {}", s, s.unwrap().days()),
                    _ => (),
                }

                let s = "adasldk".to_string();

                // match cell {
                //      ColumnData::String(Some(val)) => val.to_string(),
                //      ColumnData::I16(Some(val)) => val.to_string(),
                //      ColumnData::I32(Some(val)) => val.to_string(),
                //      ColumnData::I64(Some(val)) => val.to_string(),
                //      ColumnData::U8(Some(val)) => val.to_string(),
                //      ColumnData::F32(Some(val)) => val.to_string(),
                //      ColumnData::F64(Some(val)) => val.to_string(),
                //      ColumnData::Bit(Some(val)) => val.to_string(),
                //      _ => "Unsupported".to_string(),
                //  };
                worksheet.write_string(row_index, col_index as u16, s)?;
            }
        }

        row_index += 1;
    }

    workbook.save("output.xlsx")?;
    println!("Excel file created: output.xlsx");

    Ok(())
}
