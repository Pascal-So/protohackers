use std::ops::RangeInclusive;

pub type Species = String;

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub struct Site {
    pub id: u32,
}

pub type Population = u32;
pub type PopulationRange = RangeInclusive<Population>;

pub type Target = (Species, PopulationRange);

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum PolicyAction {
    Cull,
    Conserve,
}

pub type PolicyId = u32;
