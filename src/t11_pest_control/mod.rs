use crate::server::TcpServerProblem;

mod adapter;
mod domain;
mod port;
mod tcp_server_driver;

pub fn create_protohackers_solution() -> impl TcpServerProblem {
    let authority = adapter::authority::OnlineAuthority::new("pestcontrol.protohackers.com:20547");
    tcp_server_driver::PestControlServer::new(authority)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use adapter::mock_authority::Policy;
    use domain::types;

    use super::*;

    #[test]
    fn test_controller() {
        env_logger::init();

        let targets = HashMap::from([
            (
                types::Site { id: 1 },
                vec![
                    ("species1".to_string(), 5..=6),
                    ("species2".to_string(), 4..=8),
                ],
            ),
            (
                types::Site { id: 2 },
                vec![("species1".to_string(), 10..=20)],
            ),
        ]);

        let auth = adapter::mock_authority::MockAuthority::new(targets);
        let policies = auth.policies.clone();

        let mut controller = domain::Controller::new(auth);

        controller
            .site_visit(types::Site { id: 1 }, vec![("species1".to_string(), 5)])
            .unwrap();
        assert_eq!(
            &policies.lock().unwrap()[..],
            &[Policy {
                site: types::Site { id: 1 },
                species: "species2".to_string(),
                action: domain::types::PolicyAction::Conserve,
                policy_id: 0,
                deleted: false
            }]
        );

        controller
            .site_visit(
                types::Site { id: 1 },
                vec![("species1".to_string(), 7), ("species2".to_string(), 6)],
            )
            .unwrap();
        assert_eq!(
            &policies.lock().unwrap()[..],
            &[
                Policy {
                    site: types::Site { id: 1 },
                    species: "species2".to_string(),
                    action: domain::types::PolicyAction::Conserve,
                    policy_id: 0,
                    deleted: true
                },
                Policy {
                    site: types::Site { id: 1 },
                    species: "species1".to_string(),
                    action: domain::types::PolicyAction::Cull,
                    policy_id: 1,
                    deleted: false
                }
            ]
        );

        controller
            .site_visit(
                types::Site { id: 2 },
                vec![("species1".to_string(), 12), ("species2".to_string(), 500)],
            )
            .unwrap();
        assert_eq!(
            &policies.lock().unwrap()[..],
            &[
                Policy {
                    site: types::Site { id: 1 },
                    species: "species2".to_string(),
                    action: domain::types::PolicyAction::Conserve,
                    policy_id: 0,
                    deleted: true
                },
                Policy {
                    site: types::Site { id: 1 },
                    species: "species1".to_string(),
                    action: domain::types::PolicyAction::Cull,
                    policy_id: 1,
                    deleted: false
                }
            ]
        );
    }
}
