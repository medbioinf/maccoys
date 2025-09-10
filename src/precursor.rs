use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use serde::{Deserialize, Serialize};

/// Simple struct representing a precursor with m/z and charge
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Precursor {
    /// m/z
    mz: f64,
    /// Charge
    charge: u8,
}

impl Precursor {
    /// Creates a new Precursor
    pub fn new(mz: f64, charge: u8) -> Self {
        Self { mz, charge }
    }

    /// Returns the m/z
    pub fn mz(&self) -> f64 {
        self.mz
    }

    /// Returns the charge
    pub fn charge(&self) -> u8 {
        self.charge
    }

    pub fn to_dalton(&self) -> f64 {
        mass_to_charge_to_dalton(self.mz, self.charge)
    }
}
