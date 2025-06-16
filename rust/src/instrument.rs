use crate::error::Result;
use crate::response::ApiResponse;
use mbinary::enums::Dataset;
use mbinary::symbols::Instrument;
use mbinary::vendors::Vendors;
use reqwest::{self, Client, ClientBuilder};
use reqwest::{Response, StatusCode};
use std::time::Duration;

#[derive(Clone)]
pub struct Instruments {
    base_url: String,
    client: Client,
}

impl Instruments {
    pub fn new(base_url: &str) -> Self {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(20000)) // Set timeout to 120 seconds
            .build()
            .expect("Failed to build HTTP client");

        Instruments {
            base_url: base_url.to_string(),
            client,
        }
    }

    fn url(&self, endpoint: &str) -> String {
        format!(
            "{}{}{}",
            self.base_url,
            "/instruments/".to_string(),
            endpoint.to_string()
        )
    }

    // Instruments
    pub async fn create_symbol(&self, instrument: &Instrument) -> Result<ApiResponse<u32>> {
        let url = self.url("create");

        // Send the POST request
        let response: Response = self.client.post(&url).json(instrument).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<u32>::from_response(response).await;
        }

        let api_response = ApiResponse::<u32>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn get_symbol(
        &self,
        ticker: &String,
        dataset: &Dataset,
    ) -> Result<ApiResponse<Vec<Instrument>>> {
        let url = self.url("get");

        // Send GET request
        let response = self
            .client
            .get(&url)
            .json(&(ticker, dataset))
            .send()
            .await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<Instrument>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<Instrument>>::from_response(response).await?;
        Ok(api_response)
    }

    /// Returns data = ""
    pub async fn delete_symbol(&self, id: &i32) -> Result<ApiResponse<String>> {
        let url = self.url("delete");
        let response = self.client.delete(&url).json(id).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<String>::from_response(response).await;
        }

        let api_response = ApiResponse::<String>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn update_symbol(
        &self,
        instrument: &Instrument,
        // id: &i32,
    ) -> Result<ApiResponse<String>> {
        let url = self.url("update");
        let response = self.client.put(&url).json(&instrument).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<String>::from_response(response).await;
        }

        let api_response = ApiResponse::<String>::from_response(response).await?;
        Ok(api_response)
    }
    pub async fn list_dataset_symbols(
        &self,
        dataset: &Dataset,
    ) -> Result<ApiResponse<Vec<Instrument>>> {
        let url = self.url("list_dataset");
        let response = self.client.get(&url).json(dataset).send().await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<Instrument>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<Instrument>>::from_response(response).await?;
        Ok(api_response)
    }

    pub async fn list_vendor_symbols(
        &self,
        vendor: &Vendors,
        dataset: &Dataset,
    ) -> Result<ApiResponse<Vec<Instrument>>> {
        let url = self.url("list_vendor");
        let response = self
            .client
            .get(&url)
            .json(&(vendor, dataset))
            .send()
            .await?;

        // Check for HTTP status
        if response.status() != StatusCode::OK {
            // Deserialize the API response and return it, even if it indicates failure
            return ApiResponse::<Vec<Instrument>>::from_response(response).await;
        }

        let api_response = ApiResponse::<Vec<Instrument>>::from_response(response).await?;
        Ok(api_response)
    }
}
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use dotenv::dotenv;
    use mbinary::enums::Dataset;
    use mbinary::symbols::Instrument;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;

    async fn create_dummy_instrument(client: &Instruments) -> anyhow::Result<i32> {
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        // Create instrument
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple tester client",
            Dataset::Equities,
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

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_instrument_create() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);

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
            "AAP00001",
            "Apple tester client",
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1,
            1,
            1,
            false,
            true,
        );

        // Test
        let response = client.create_symbol(&instrument).await?;
        let id = response.data as i32;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_instrument_create_error() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
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
            "AAPL",
            "Apple tester client",
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1,
            1,
            1,
            false,
            true,
        );

        let response = client.create_symbol(&instrument).await?;

        // Validate
        assert_eq!(response.code, 500);
        assert_eq!(response.status, "failed");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_instrument() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let response = client
            .get_symbol(&"AAPL".to_string(), &Dataset::Equities)
            .await?;

        println!("{:?}", response);

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");
        assert!(response.data.len() > 0);

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_get_instrument_none() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);

        // Test
        let response = client
            .get_symbol(&("AAPL9".to_string()), &Dataset::Equities)
            .await?;

        // Validate
        assert_eq!(response.code, 404); // Request was valid but that ticker doesnt exist
        assert_eq!(response.status, "success");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_update_instrument() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let instrument = Instrument::new(
            Some(id as u32),
            "TTT0005",
            "New name",
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1,
            2,
            1,
            false,
            true,
        );

        let response = client.update_symbol(&instrument).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_list_dataset_instruments() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let dataset = Dataset::Equities;
        let response = client.list_dataset_symbols(&dataset).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_list_vendor_instruments() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("MIDAS_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let dataset = Dataset::Equities;
        let vendor = Vendors::Databento;
        let response = client.list_vendor_symbols(&vendor, &dataset).await?;

        // Validate
        assert_eq!(response.code, 200);
        assert_eq!(response.status, "success");

        // Cleanup
        let _ = client.delete_symbol(&id).await?;

        Ok(())
    }
}
