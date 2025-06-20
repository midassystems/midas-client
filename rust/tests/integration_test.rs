use dotenv::dotenv;
use mbinary::encode::CombinedEncoder;
use mbinary::enums::Dataset;
use mbinary::enums::{Action, Schema};
use mbinary::metadata::Metadata;
use mbinary::record_ref::RecordRef;
use mbinary::records::{BidAskPair, Mbp1Msg, RecordHeader};
use mbinary::symbols::{Instrument, SymbolMap};
use mbinary::vendors::Vendors;
use mbinary::vendors::{DatabentoData, VendorData};
use midas_client::historical::Historical;
use midas_client::instrument::Instruments;
use serial_test::serial;
use std::path::PathBuf;
use std::str::FromStr;
// use mbinary::params::RetrieveParams;

async fn create_dummy_instrument(ticker: &str, dataset: Dataset) -> anyhow::Result<i32> {
    dotenv().ok();
    let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
    let client = Instruments::new(&base_url);

    // Create instrument
    let schema = dbn::Schema::from_str("mbp-1")?;
    let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset: dbn_dataset,
        stype,
    });

    let instrument = Instrument::new(
        None,
        ticker,
        "Apple tester client",
        dataset,
        Vendors::Databento,
        vendor_data.encode(),
        1,
        1,
        1,
        false,
        true,
    );

    let create_response = client.create_symbol(&instrument).await?;
    let id = create_response.data as i32;

    Ok(id)
}

async fn delete_dummy_instrument(id: &i32) -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
    let client = Instruments::new(&base_url);
    let _ = client.delete_symbol(&id).await?;

    Ok(())
}

async fn create_dummy_records_file(
    ticker: &str,
    dataset: Dataset,
    filename: &PathBuf,
) -> anyhow::Result<i32> {
    dotenv().ok();
    let id = create_dummy_instrument(ticker, dataset).await?;

    // Pull test data
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6770,
        size: 1,
        action: Action::Trade as i8,
        side: 2,
        depth: 0,
        flags: 0,
        ts_recv: 1704209103644092564,
        ts_in_delta: 17493,
        sequence: 739763,
        discriminator: 0,
        levels: [BidAskPair {
            ask_px: 1,
            bid_px: 1,
            bid_sz: 2,
            ask_sz: 2,
            bid_ct: 10,
            ask_ct: 20,
        }],
    };
    let mbp_2 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092565, 0) },
        price: 6870,
        size: 2,
        action: Action::Trade as i8,
        side: 1,
        depth: 0,
        flags: 0,
        ts_recv: 1704209103644092565,
        ts_in_delta: 17493,
        sequence: 739763,
        discriminator: 0,
        levels: [BidAskPair {
            ask_px: 1,
            bid_px: 1,
            bid_sz: 2,
            ask_sz: 2,
            bid_ct: 10,
            ask_ct: 20,
        }],
    };
    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_2).into();

    let metadata = Metadata::new(
        Schema::Mbp1,
        Dataset::Equities,
        1704209103644092564,
        1704209103644092566,
        SymbolMap::new(),
    );

    let mut buffer = Vec::new();
    let mut encoder = CombinedEncoder::new(&mut buffer);
    encoder.encode_metadata(&metadata)?;
    encoder
        .encode_records(&[record_ref1, record_ref2])
        .expect("Encoding failed");

    // Create records file
    let _ = encoder.write_to_file(filename, false)?;

    Ok(id)
}

// -- Tests
#[tokio::test]
#[serial]
// #[ignore]
async fn test_create_mbp_from_file() -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
    let client = Historical::new(&base_url);

    let filename = "midas_client_test_mbp-1.bin";
    let path = PathBuf::from("../../midas-server/data/processed_data").join(filename);
    let ticker = "AAPL";
    let dataset = Dataset::Equities;
    let id = create_dummy_records_file(ticker, dataset, &path).await?;

    // Test
    let result = client.create_mbp_from_file(filename).await?;

    // Validate
    assert_eq!(result.status, "success");

    // Cleanup
    let _ = delete_dummy_instrument(&id).await?;

    let _ = tokio::fs::remove_file(path).await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_create_mbp_from_file_duplicate_error() -> anyhow::Result<()> {
    dotenv().ok();
    let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
    let client = Historical::new(&base_url);

    let filename = "midas_client_test_mbp-1.bin";
    let path = PathBuf::from("../../midas-server/data/processed_data").join(filename);

    let ticker = "AAPL";
    let dataset = Dataset::Equities;

    let id = create_dummy_instrument(ticker, dataset).await?;

    // Pull test data
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6770,
        size: 1,
        action: Action::Trade as i8,
        side: 2,
        depth: 0,
        flags: 0,
        ts_recv: 1704209103644092564,
        ts_in_delta: 17493,
        sequence: 739763,
        discriminator: 0,
        levels: [BidAskPair {
            ask_px: 1,
            bid_px: 1,
            bid_sz: 2,
            ask_sz: 2,
            bid_ct: 10,
            ask_ct: 20,
        }],
    };

    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_1).into();

    let metadata = Metadata::new(
        Schema::Mbp1,
        Dataset::Equities,
        1704209103644092564,
        1704209103644092566,
        SymbolMap::new(),
    );

    let mut buffer = Vec::new();
    let mut encoder = CombinedEncoder::new(&mut buffer);
    encoder.encode_metadata(&metadata)?;
    encoder
        .encode_records(&[record_ref1, record_ref2])
        .expect("Encoding failed");

    // Create records file
    let _ = encoder.write_to_file(&path, false)?;

    // Test
    let result = client.create_mbp_from_file(filename).await?;

    // Validate
    assert_eq!(result.status, "failed");

    // Cleanup
    let _ = delete_dummy_instrument(&id).await?;
    let _ = tokio::fs::remove_file(path).await;

    Ok(())
}
