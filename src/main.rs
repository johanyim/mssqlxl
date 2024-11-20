use futures::TryStreamExt;
use rust_xlsxwriter::Workbook;
use tiberius::{AuthMethod, Client, Config, QueryItem, QueryStream};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let query = "SELECT * from claims_scr";

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

    // Step 3: Prepare Excel workbook
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();

    // Step 4: Write data to the worksheet
    let mut row_index = 0;

    while let Some(result_row) = stream.try_next().await? {
        let row: QueryItem = result_row;
        if row_index == 0 {
            // Write column headers for the first row
            for (col_index, column) in row.as_metadata().unwrap().columns().iter().enumerate() {
                worksheet.write_string(0, col_index as u16, column.name())?;
            }
            row_index += 1;
        }

        // // Write data for each subsequent row
        // for (col_index, value) in row.into_iter().enumerate() {
        //     let cell_value = match value {
        //         Some(tiberius::ColumnData::String(Some(val))) => val,
        //         Some(tiberius::ColumnData::I32(Some(val))) => val.to_string(),
        //         Some(tiberius::ColumnData::F64(Some(val))) => val.to_string(),
        //         Some(_) => "Unsupported".to_string(),
        //         None => "NULL".to_string(),
        //     };
        //     worksheet.write_string(row_index, col_index as u16, &cell_value, None)?;
        // }
        // row_index += 1;
    }

    // Step 5: Save the Excel file
    workbook.save("output.xlsx")?;
    println!("Excel file created: output.xlsx");

    Ok(())
}
