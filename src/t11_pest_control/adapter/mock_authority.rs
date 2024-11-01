use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::bail;

use crate::t11_pest_control::{
    domain::types,
    port::authority::{AuthorityServer, AuthoritySession},
};

pub struct MockAuthority {
    pub policies: Arc<Mutex<Vec<Policy>>>,
    targets: HashMap<types::Site, Vec<types::Target>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub site: types::Site,
    pub species: types::Species,
    pub action: types::PolicyAction,
    pub policy_id: types::PolicyId,
    pub deleted: bool,
}

impl MockAuthority {
    pub fn new(targets: HashMap<types::Site, Vec<types::Target>>) -> MockAuthority {
        MockAuthority {
            policies: Arc::new(Mutex::new(vec![])),
            targets,
        }
    }
}

impl AuthorityServer for MockAuthority {
    type Session = MockAuthoritySession;

    fn dial(
        &self,
        site: types::Site,
    ) -> Result<(Self::Session, Vec<types::Target>), anyhow::Error> {
        let session = Self::Session {
            policies: self.policies.clone(),
            site,
        };
        let targets = self.targets.get(&site).unwrap().clone();
        Ok((session, targets))
    }
}

pub struct MockAuthoritySession {
    policies: Arc<Mutex<Vec<Policy>>>,
    site: types::Site,
}

impl AuthoritySession for MockAuthoritySession {
    fn create_policy(
        &mut self,
        species: types::Species,
        action: types::PolicyAction,
    ) -> Result<types::PolicyId, anyhow::Error> {
        let mut policies = self.policies.lock().unwrap();
        let policy_id = policies.len() as types::PolicyId;

        if policies
            .iter()
            .any(|policy| policy.species == species && !policy.deleted && policy.site == self.site)
        {
            bail!("policy for species {species} already exists");
        }

        policies.push(Policy {
            site: self.site,
            species,
            action,
            policy_id,
            deleted: false,
        });

        Ok(policy_id)
    }

    fn delete_policy(&mut self, policy_id: types::PolicyId) -> Result<(), anyhow::Error> {
        let mut policies = self.policies.lock().unwrap();
        if let Some(policy) = policies.iter_mut().find(|policy| {
            policy.policy_id == policy_id && !policy.deleted && policy.site == self.site
        }) {
            policy.deleted = true;
            Ok(())
        } else {
            bail!(
                "no policy with id {policy_id} exists for site {}",
                self.site.id
            );
        }
    }
}
