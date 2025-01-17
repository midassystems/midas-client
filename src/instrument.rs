use crate::error::Result;
use crate::response::ApiResponse;
use mbn::enums::{Dataset, Vendors};
use mbn::symbols::Instrument;
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
    use super::*;
    use crate::error::Result;
    use dotenv::dotenv;
    use mbn::enums::Dataset;
    use mbn::enums::Vendors;
    use mbn::symbols::Instrument;
    use serial_test::serial;

    async fn create_dummy_instrument(client: &Instruments) -> Result<i32> {
        // Create instrument
        let instrument = Instrument::new(
            None,
            "AAPL9",
            "Apple tester client",
            Vendors::Databento,
            Dataset::Equities,
            1,
            1,
            true,
        );

        let create_response = client.create_symbol(&instrument).await?;
        let id = create_response.data as i32;

        Ok(id)
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_instrument_create() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);

        let instrument = Instrument::new(
            None,
            "AAP00001",
            "Apple tester client",
            Vendors::Databento,
            Dataset::Equities,
            1,
            1,
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
    async fn test_instrument_create_error() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let instrument = Instrument::new(
            None,
            "AAPL9",
            "Apple tester client",
            Vendors::Databento,
            Dataset::Equities,
            1,
            1,
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
    async fn test_get_instrument() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let response = client
            .get_symbol(&"AAPL9".to_string(), &Dataset::Equities)
            .await?;

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
    async fn test_get_instrument_none() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);

        // Test
        let response = client
            .get_symbol(&("AAPL".to_string()), &Dataset::Equities)
            .await?;

        // Validate
        assert_eq!(response.code, 404); // Request was valid but that ticker doesnt exist
        assert_eq!(response.status, "success");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_update_instrument() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);
        let id = create_dummy_instrument(&client).await?;

        // Test
        let instrument = Instrument::new(
            Some(id as u32),
            "TTT0005",
            "New name",
            Vendors::Databento,
            Dataset::Equities,
            1,
            2,
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
    async fn test_list_dataset_instruments() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
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
    async fn test_list_vendor_instruments() -> Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
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
