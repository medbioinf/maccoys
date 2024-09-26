/// A record for the goodness of fit test results
///
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct GoodnessOfFitRecord {
    /// The name of the goodness of fit test
    test_name: String,
    /// Name of the distribution of the test
    distribution: String,
    /// Staticics of the test
    statictcs: f64,
    /// P-value of the test
    p_value: f64,
}

impl GoodnessOfFitRecord {
    /// Returns the test name
    ///
    pub fn get_test_name(&self) -> &str {
        &self.test_name
    }

    /// Returns the distribution name
    ///
    pub fn get_distribution(&self) -> &str {
        &self.distribution
    }

    /// Returns the statictics of the test
    ///
    pub fn get_statictcs(&self) -> f64 {
        self.statictcs
    }

    /// Returns the p-value of the test
    ///
    pub fn get_p_value(&self) -> f64 {
        self.p_value
    }
}

impl From<(String, String, f64, f64)> for GoodnessOfFitRecord {
    fn from((test_name, distribution, statictcs, p_value): (String, String, f64, f64)) -> Self {
        Self {
            test_name,
            distribution,
            statictcs,
            p_value,
        }
    }
}
