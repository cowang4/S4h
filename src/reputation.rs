

use crate::{key::Key, rpc::{ComplaintData}};


#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WitnessReport {
    /// node_id of witness
    pub a: Key,
    /// complaints received, or number of complaints against node q
    pub cr: usize,
    /// complaints filed, or number of complaints by node q
    pub cf: usize,
}

impl WitnessReport {
    pub fn from_complaint_data(a: Key, data: ComplaintData) -> WitnessReport {
        WitnessReport {
            a: a,
            cr: data.0.len(),
            cf: data.1.len(),
        }
    }
}
