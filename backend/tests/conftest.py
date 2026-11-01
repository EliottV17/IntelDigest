def pytest_addoption(parser):
    parser.addoption(
        "--test-database-url", default=None,
        help="Opt in to serial PostgreSQL tests on local inteldigest_test only. "
             "Requires explicit operator authorization; no dotenv is loaded.",
    )
