// std import
use std::fmt;

// 3rd party imports
use dihardts_omicstools::proteomics::post_translational_modifications::{
    ModificationType, Position as ModificationPosition,
};

#[derive(Clone)]
pub struct DecoyPart {
    index: usize,
    amino_acid: char,
    amino_acid_mass: i64,
    modification_type: Option<ModificationType>,
    modification_position: ModificationPosition,
    modification_mass: i64,
    total_mass: i64,
}

impl DecoyPart {
    pub fn new(
        index: usize,
        amino_acid: char,
        amino_acid_mass: i64,
        modification_type: Option<ModificationType>,
        modification_position: ModificationPosition,
        modification_mass: i64,
    ) -> Self {
        let total_mass = amino_acid_mass + modification_mass;
        Self {
            index,
            amino_acid,
            amino_acid_mass,
            modification_type,
            modification_position,
            modification_mass,
            total_mass,
        }
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    pub fn get_amino_acid(&self) -> char {
        self.amino_acid
    }

    pub fn get_amino_acid_mass(&self) -> i64 {
        self.amino_acid_mass
    }

    pub fn get_modification_type(&self) -> &Option<ModificationType> {
        &self.modification_type
    }

    pub fn get_modification_position(&self) -> &ModificationPosition {
        &self.modification_position
    }

    pub fn get_modification_mass(&self) -> i64 {
        self.modification_mass
    }

    pub fn get_total_mass(&self) -> i64 {
        self.total_mass
    }
}

impl fmt::Display for DecoyPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.modification_type {
            Some(mod_type) => {
                let modification_marker = match mod_type {
                    ModificationType::Static => "s",
                    ModificationType::Variable => "v",
                };
                write!(
                    f,
                    "{}[{}{}]",
                    self.amino_acid, modification_marker, self.modification_mass
                )
            }
            None => write!(f, "{}", self.amino_acid),
        }
    }
}

impl PartialEq for DecoyPart {
    fn eq(&self, other: &Self) -> bool {
        self.amino_acid == other.amino_acid
            && self.modification_type == other.modification_type
            && self.modification_mass == other.modification_mass
            && self.amino_acid_mass == other.amino_acid_mass
    }
}
