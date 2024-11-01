use crate::t11_pest_control::domain::types;

pub trait AuthorityServer {
    type Session: AuthoritySession;

    fn dial(&self, site: types::Site)
        -> Result<(Self::Session, Vec<types::Target>), anyhow::Error>;
}

pub trait AuthoritySession {
    fn create_policy(
        &mut self,
        species: types::Species,
        action: types::PolicyAction,
    ) -> Result<types::PolicyId, anyhow::Error>;

    fn delete_policy(&mut self, policy_id: types::PolicyId) -> Result<(), anyhow::Error>;
}
