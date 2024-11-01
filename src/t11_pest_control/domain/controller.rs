//! The core of the pest control server.
//!
//! This module is independent of the protocol implementation. It exposes methods that are called
//! by the client adapters, and calls to the server through the `AuthorityServer` abstraction.

use std::collections::{hash_map::Entry, HashMap};

use anyhow::Context;
use thiserror::Error;

use crate::t11_pest_control::port::authority::{AuthorityServer, AuthoritySession};

use super::types::{PolicyAction, PolicyId, Population, Site, Species, Target};

pub struct Controller<Authority: AuthorityServer> {
    authority: Authority,
    sites: HashMap<
        Site,
        (
            Vec<Target>,
            Authority::Session,
            HashMap<Species, (PolicyId, PolicyAction)>,
        ),
    >,
}

#[derive(Error, Debug)]
pub enum SiteVisitError {
    #[error("duplicate observations")]
    DuplicateObservations,

    #[error("internal error: {0}")]
    InternalError(#[from] anyhow::Error),
}

impl<Authority: AuthorityServer> Controller<Authority> {
    pub fn new(authority: Authority) -> Self {
        Self {
            authority,
            sites: HashMap::new(),
        }
    }

    pub fn site_visit(
        &mut self,
        site: Site,
        mut observations: Vec<(Species, Population)>,
    ) -> Result<(), SiteVisitError> {
        let entry = self.sites.entry(site);

        let (targets, session, policies) = match entry {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                let (session, targets) = self
                    .authority
                    .dial(site)
                    .context("while dialling authority")?;
                vacant.insert((targets, session, HashMap::new()))
            }
        };

        observations.sort();
        observations.dedup();

        let has_duplicate = observations
            .iter()
            .fold((false, None), |(has_dup, last), (species, _)| {
                let has_dup = has_dup || Some(species) == last;
                (has_dup, Some(species))
            })
            .0;

        if has_duplicate {
            return Err(SiteVisitError::DuplicateObservations);
        }

        for (species, range) in targets.iter() {
            let count = match observations
                .iter()
                .find(|obs| obs.0 == *species)
            {
                Some((_, count)) => *count,
                None => 0,
            };

            let new_action = if range.contains(&count) {
                None
            } else if count < *range.start() {
                Some(PolicyAction::Conserve)
            } else {
                Some(PolicyAction::Cull)
            };

            log::info!("new action for site {}, {species}: {new_action:?}", site.id);

            match (policies.get(species), new_action) {
                (None, Some(new_action)) => {
                    let id = session.create_policy(species.clone(), new_action).context("creating new policy")?;
                    policies.insert(species.clone(), (id, new_action));
                }
                (Some((id, action)), Some(new_action)) if action != &new_action => {
                    session.delete_policy(*id).context("deleting old policy")?;
                    let id = session.create_policy(species.clone(), new_action).context("creating new policy")?;
                    policies.insert(species.clone(), (id, new_action));
                }
                (Some((id, _)), None) => {
                    session.delete_policy(*id).context("deleting old policy")?;
                    policies.remove(species);
                }
                _ => {}
            }
        }

        Ok(())
    }
}
