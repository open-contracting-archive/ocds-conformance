# Open Contracting Data Standard: Conformance

Checks the conformance of known implementations to [OCDS](http://standard.open-contracting.org/), and reports which classes and properties are unused.

## Getting Started

    bundle
    bundle exec rake check[URI]

Examples:

    bundle exec rake check[https://github.com/devgateway/ca-app-ocds-export]
    bundle exec rake check[https://raw.githubusercontent.com/open-contracting/sample-data/master/buyandsell/ocds_data/complete_record.json.zip]

## Bugs? Questions?

This project's main repository is on GitHub: [http://github.com/opennorth/ocds-conformance](http://github.com/opennorth/ocds-conformance), where your contributions, forks, bug reports, feature requests, and feedback are greatly welcomed.

Copyright (c) 2014 Open North Inc., released under the MIT license
